package name.zicat.relax.nedis.sentinel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.Logger;

import com.newegg.ec.nedis.utils.NedisException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

/***
 * The jedis pool supports sentine feature
 * 
 * @company Newegg Tech (China) Co, Ltd
 * @author lz31
 * @date 2014-2-25
 */
public class JedisSentinelSlavePool extends JedisPool<Jedis> {
	protected Logger log = Logger.getLogger(getClass().getName());

	protected GenericObjectPoolConfig poolConfig;
	protected int timeout = Protocol.DEFAULT_TIMEOUT;
	protected String password;
	
	protected SlaveListener slaveListener;
	protected int reflashWaitTimeMillis;

	/** The current count of slave **/
	protected int currentSlaveSize;

	public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
			int reflashWaitTimeMillis) {
		this(masterName, sentinels, new GenericObjectPoolConfig(),
				Protocol.DEFAULT_TIMEOUT, null, reflashWaitTimeMillis);
	}

	public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
			int reflashWaitTimeMillis, GenericObjectPoolConfig config) {
		this(masterName, sentinels, config, Protocol.DEFAULT_TIMEOUT, null,
				reflashWaitTimeMillis);
	}
	
	public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
			int reflashWaitTimeMillis, GenericObjectPoolConfig config, int timeout) {
		this(masterName, sentinels, config, timeout, null,
				reflashWaitTimeMillis);
	}
	
	public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
			final GenericObjectPoolConfig poolConfig, int timeout,
			final String password, final int reflashWaitTimeMillis) {
		this.poolConfig = poolConfig;
		this.timeout = timeout;
		this.password = password;
		this.reflashWaitTimeMillis = reflashWaitTimeMillis;

		// get all slaves host and port infomation by sentinels according to
		// masterName,and start the slave and master listener
		List<HostAndPort> slaves = initSentinels(sentinels, masterName);

		// init pool by slaves
		if (slaves.isEmpty()) {
			throw new NedisException("init pool error, get slaves info failed");
		}
		initPool(slaves);
	}

	/***
	 * init pool by slaves host and port
	 * 
	 * @param slaves
	 */
	private synchronized void initPool(List<HostAndPort> slaves) {
		if (currentSlaveSize != slaves.size()) {
			currentSlaveSize = slaves.size();
			log.info("Created JedisPool to slaves at " + getSlaveInfo(slaves));
			initPool(poolConfig, new RandomJedisFactory(slaves,
					password, timeout));
		}
	}

	/***
	 * init pool by slaves host and port
	 * 
	 * @param slaves
	 */
	private synchronized void initPool(List<HostAndPort> slaves, AtomicBoolean b) {
		if (currentSlaveSize != slaves.size()) {
			log.warn("slave count different. original:"+ currentSlaveSize +" current:"+ slaves.size());
			currentSlaveSize = slaves.size();
			log.info("Created JedisPool to slaves at " + getSlaveInfo(slaves));
			if (b.get()) {
				initPool(poolConfig, new RandomJedisFactory(slaves,
						password, timeout));
				b.set(true);
			}
		}
	}
	
	private String getSlaveInfo(List<HostAndPort> slaves) {
		StringBuffer sb = new StringBuffer();
		for(HostAndPort slave: slaves) {
			sb.append(slave.toString());
			sb.append(" ;");
		}
		return sb.toString();
	}
	
	
	public void returnBrokenResource(final Jedis resource) {
		returnBrokenResourceObject(resource);
	}

	public void returnResource(final Jedis resource) {
		returnResourceObject(resource);
	}

	public void destroy() {

		slaveListener.shutdown();
		super.destroy();
	}

	/***
	 * get all slaves host and port infomation by sentinels according to
	 * masterName and start the slave and master listener
	 * 
	 * @param sentinels
	 * @param masterName
	 * @return
	 */
	private List<HostAndPort> initSentinels(Set<String> sentinels, String masterName) {
		
		log.info("Trying to find slaves from available Sentinels...");
		List<HostAndPort> slaves = new ArrayList<HostAndPort>();

		for (String sentinel : sentinels) {

			final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
			Jedis jedis = null;
			try {
				jedis = new Jedis(hap.host, hap.port);
				List<Map<String, String>> slaveInfo = jedis.sentinelSlaves(masterName);
				
				for (Map<String, String> entry : slaveInfo) {

					String[] flagInfo = entry.get("flags").split(",");
					if(flagInfo.length == 1 && "slave".equalsIgnoreCase(flagInfo[0])) {
						HostAndPort hostAndPort = new HostAndPort();
						hostAndPort.host = entry.get("ip");
						hostAndPort.port = Integer.valueOf(entry.get("port").trim());
						slaves.add(hostAndPort);
					}
				}
				break;
			} catch (Exception e) {
				log.error("Cannot connect to sentinel running @ " + hap + ". Try next one.");
			} finally {
				if(jedis != null) {
					try {
						jedis.quit();
					} catch(Exception e) {}
					finally {
						try {
							jedis.disconnect();
						} catch(Exception e) {}
					} 
				}
			}
		}
		
		if(slaves.isEmpty()) {
			throw new NedisException("all sentinels maybe shutdown, please check!!!");
		}
		
		List<HostAndPort> sentinelsInfo = new ArrayList<HostAndPort>();
		for(String str: sentinels) {
			sentinelsInfo.add(toHostAndPort(Arrays.asList(str.split(":"))));
		}
		slaveListener = new SlaveListener(masterName, sentinelsInfo, this.reflashWaitTimeMillis);
		slaveListener.setDaemon(true);
		slaveListener.start();

		return slaves;
	}

	private HostAndPort toHostAndPort(List<String> getSlaveAddrbyNameReulst) {

		final HostAndPort hap = new HostAndPort();
		hap.host = getSlaveAddrbyNameReulst.get(0);
		hap.port = Integer.parseInt(getSlaveAddrbyNameReulst.get(1));

		return hap;
	}

	/**
	 * Slave listener to listener the change event of slaves
	 * 
	 * @company Newegg Tech (China) Co, Ltd
	 * @author lz31
	 * @date 2014-2-25
	 */
	protected class SlaveListener extends Thread {

		protected String masterName;
		protected List<HostAndPort> slaves;
		protected int reflashWaitTimeMillis = 1000;
		protected AtomicBoolean running = new AtomicBoolean(false);
		public final Random random = new Random();
		
		public SlaveListener(String masterName, List<HostAndPort> slaves) {
			this.masterName = masterName;
			this.slaves = slaves;
		}
		
		
		public SlaveListener(String masterName, List<HostAndPort> slaves,
				int reflashWaitTimeMillis) {
			this(masterName, slaves);
			this.reflashWaitTimeMillis = reflashWaitTimeMillis;
		}

		@Override
		public void run() {
			running.set(true);
			while (running.get()) {
				HostAndPort hostAndPortSen = slaves.get(random.nextInt(slaves.size()));
				Jedis jedis = null;
				try {
					
					jedis = new Jedis(hostAndPortSen.host, hostAndPortSen.port);
					
					List<HostAndPort> slaves = new ArrayList<HostAndPort>();
					List<Map<String, String>> slaveInfo = jedis
							.sentinelSlaves(masterName);

					// collect the slaves info which is connected and the status
					// is not err
					for (Map<String, String> entry : slaveInfo) {
						String[] flagInfo = entry.get("flags").split(",");
						
						if(flagInfo.length == 1 && "slave".equalsIgnoreCase(flagInfo[0])) {
							HostAndPort hostAndPort = new HostAndPort();
							hostAndPort.host = entry.get("ip");
							hostAndPort.port = Integer.valueOf(entry.get(
									"port").trim());
							slaves.add(hostAndPort);
						}
					}
					if(!slaves.isEmpty()) {
						initPool(slaves, running);
					}
				} catch (Exception e) {
				} finally {
					if(jedis != null) {
						try {
							jedis.quit();
						} catch (Exception e) {} 
						finally {
							try {
								jedis.disconnect();
							} catch (Exception e) {
							}
						}
					}
				}
				try {
					Thread.sleep(reflashWaitTimeMillis);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		/**
		 * shutdown the listener
		 */
		public void shutdown() {
			try {
				running.set(false);
			} catch (Exception e) {
			}
		}
	}
}
