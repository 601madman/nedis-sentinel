package name.zicat.relax.nedis.sentinel;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class JedisSentinelMasterPool extends JedisPool<Jedis> {
	protected GenericObjectPoolConfig poolConfig;

	protected int timeout = Protocol.DEFAULT_TIMEOUT;

	protected String password;

	protected int database = Protocol.DEFAULT_DATABASE;

	protected Set<MasterListener> masterListeners = new HashSet<MasterListener>();

	public static Logger log = Logger.getLogger(JedisSentinelMasterPool.class);

	public JedisSentinelMasterPool(String masterName, Set<String> sentinels,
			final GenericObjectPoolConfig poolConfig) {
		this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, null,
				Protocol.DEFAULT_DATABASE);
	}
	
	public JedisSentinelMasterPool(String masterName, Set<String> sentinels) {
		this(masterName, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT,
				null, Protocol.DEFAULT_DATABASE);
	}

	public JedisSentinelMasterPool(String masterName, Set<String> sentinels,
			String password) {
		this(masterName, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT,
				password);
	}

	public JedisSentinelMasterPool(String masterName, Set<String> sentinels,
			final GenericObjectPoolConfig poolConfig, int timeout, final String password) {
		this(masterName, sentinels, poolConfig, timeout, password,
				Protocol.DEFAULT_DATABASE);
	}

	public JedisSentinelMasterPool(String masterName, Set<String> sentinels,
			final GenericObjectPoolConfig poolConfig, final int timeout) {
		this(masterName, sentinels, poolConfig, timeout, null,
				Protocol.DEFAULT_DATABASE);
	}

	public JedisSentinelMasterPool(String masterName, Set<String> sentinels,
			final GenericObjectPoolConfig poolConfig, final String password) {
		this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT,
				password);
	}

	public JedisSentinelMasterPool(String masterName, Set<String> sentinels,
			final GenericObjectPoolConfig poolConfig, int timeout, final String password,
			final int database) {
		this.poolConfig = poolConfig;
		this.timeout = timeout;
		this.password = password;
		this.database = database;
		HostAndPort master = initSentinels(sentinels, masterName);
		initPool(master);
	}

	public void returnBrokenResource(final Jedis resource) {
		returnBrokenResourceObject(resource);
	}

	public void returnResource(final Jedis resource) {
		returnResourceObject(resource);
	}

	private class HostAndPort {
		String host;
		int port;

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof HostAndPort) {
				HostAndPort hp = (HostAndPort) obj;
				return port == hp.port && host.equals(hp.host);
			}
			return false;
		}

		@Override
		public String toString() {
			return host + ":" + port;
		}
	}

	private volatile HostAndPort currentHostMaster;

	public void destroy() {
		for (MasterListener m : masterListeners) {
			m.shutdown();
		}

		super.destroy();
	}

	public HostAndPort getCurrentHostMaster() {
		return currentHostMaster;
	}

	private synchronized void initPool(HostAndPort master) {
		if (!master.equals(currentHostMaster)) {
			currentHostMaster = master;
			log.info("Created JedisPool to master at " + master);
			initPool(poolConfig, new GenericJedisFactory(master.host, master.port,
					timeout, password, database));
		}
	}
	
	private synchronized void initPool(HostAndPort master, boolean isFirst) {
		if (!master.equals(currentHostMaster)) {
			log.error("master addresss changed. original-" + currentHostMaster + " current-" + master);
			currentHostMaster = master;
			log.info("Created JedisPool to master at " + master);
			initPool(poolConfig, new GenericJedisFactory(master.host, master.port,
					timeout, password, database));
		}
	}
	
	private HostAndPort initSentinels(Set<String> sentinels,
			final String masterName) {

		HostAndPort master = null;
		boolean running = true;

		outer: while (running) {

			log.info("Trying to find master from available Sentinels...");

			for (String sentinel : sentinels) {

				final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel
						.split(":")));

				Jedis jedis = null;
				try {
					jedis = new Jedis(hap.host, hap.port);
					if (master == null) {
						master = toHostAndPort(jedis
								.sentinelGetMasterAddrByName(masterName));
						
						break outer;
					}
				} catch (JedisConnectionException e) {
					log.error("Cannot connect to sentinel running @ " + hap
							+ ". Trying next one.");
				} finally {
					if(jedis != null) {
						try {
							jedis.quit();
						} catch (Exception e) {}
						finally {
							try {
								jedis.disconnect();
							} catch (Exception e) {}
						}
					}
				}
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		for (String sentinel : sentinels) {
			final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel
					.split(":")));
			MasterListener masterListener = new MasterListener(masterName,
					hap.host, hap.port);
			masterListeners.add(masterListener);
			masterListener.start();
		}

		return master;
	}

	private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
		final HostAndPort hap = new HostAndPort();
		hap.host = getMasterAddrByNameResult.get(0);
		hap.port = Integer.parseInt(getMasterAddrByNameResult.get(1));
		return hap;
	}

	protected class JedisPubSubAdapter extends JedisPubSub {
		@Override
		public void onMessage(String channel, String message) {
		}

		@Override
		public void onPMessage(String pattern, String channel, String message) {
		}

		@Override
		public void onPSubscribe(String pattern, int subscribedChannels) {
		}

		@Override
		public void onPUnsubscribe(String pattern, int subscribedChannels) {
		}

		@Override
		public void onSubscribe(String channel, int subscribedChannels) {
		}

		@Override
		public void onUnsubscribe(String channel, int subscribedChannels) {
		}
	}

	protected class MasterListener extends Thread {

		protected String masterName;
		protected String host;
		protected int port;
		protected long subscribeRetryWaitTimeMillis = 5000;
		protected Jedis j;
		protected AtomicBoolean running = new AtomicBoolean(false);
		
		protected MasterListener() {
		}

		public MasterListener(String masterName, String host, int port) {
			this.masterName = masterName;
			this.host = host;
			this.port = port;
		}

		public MasterListener(String masterName, String host, int port,
				long subscribeRetryWaitTimeMillis) {
			this(masterName, host, port);
			this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
		}

		public void run() {

			running.set(true);

			while (running.get()) {
				
				j = new Jedis(host, port);

				try {
					j.subscribe(new JedisPubSubAdapter() {
						@Override
						public void onMessage(String channel, String message) {

							String[] switchMasterMsg = message.split(" ");

							if (switchMasterMsg.length > 3) {

								if (masterName.equals(switchMasterMsg[0])) {
									initPool(toHostAndPort(Arrays.asList(
											switchMasterMsg[3],
											switchMasterMsg[4])), true);
								} 

							} 
						}
					}, "+switch-master");

				} catch (JedisConnectionException e) {
					if (running.get()) {
						try {
							Thread.sleep(subscribeRetryWaitTimeMillis);
						} catch (InterruptedException e1) {
						}
					} 
				} finally {
					try {
						j.quit();
					} catch (Exception e) {} 
					finally {
						try {
							j.disconnect();
						} catch (Exception e) {} {}
					}
				}
			}
		}

		public void shutdown() {
			try {
				running.set(false);
				// This isn't good, the Jedis object is not thread safe
				j.disconnect();
			} catch (Exception e) {
			}
		}
	}
}