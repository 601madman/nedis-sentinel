package name.zicat.relax.nedis.sentinel;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import redis.clients.jedis.Jedis;

public class GenericJedisFactory implements PooledObjectFactory<Jedis> {

	private final String host;
	private final int port;
	private final int timeout;
	private final String password;
	private final int database;

	public GenericJedisFactory(final String host, final int port,
			final int timeout, final String password, final int database) {
		super();
		this.host = host;
		this.port = port;
		this.timeout = timeout;
		this.password = password;
		this.database = database;
	}

	@Override
	public PooledObject<Jedis> makeObject() throws Exception {
		final Jedis jedis = new Jedis(this.host, this.port, this.timeout);

		jedis.connect();
		if (null != this.password) {
			jedis.auth(this.password);
		}
		if (database != 0) {
			jedis.select(database);
		}
		return new DefaultPooledObject<Jedis>(jedis);
	}

	@Override
	public void destroyObject(PooledObject<Jedis> p) throws Exception {
		final Jedis jedis = p.getObject();
		if (jedis.isConnected()) {
			try {
				try {
					jedis.quit();
				} catch (Exception e) {
				}
				jedis.disconnect();
			} catch (Exception e) {

			}
		}

	}

	@Override
	public boolean validateObject(PooledObject<Jedis> p) {
		final Jedis jedis = p.getObject();
		try {
			return jedis.isConnected() && jedis.ping().equals("PONG");
		} catch (final Exception e) {
			return false;
		}
	}

	@Override
	public void activateObject(PooledObject<Jedis> p) throws Exception {

	}

	@Override
	public void passivateObject(PooledObject<Jedis> p) throws Exception {

	}

}
