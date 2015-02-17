package name.zicat.relax.nedis.sentinel;

import java.util.List;
import java.util.Random;


import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import redis.clients.jedis.Jedis;

/**
 * The circle jedis Factory to create jedis obj 
 * @company Newegg Tech (China) Co, Ltd
 * @author lz31
 * @date 2014-2-25
 */
public class RandomJedisFactory implements PooledObjectFactory<Jedis> {
	public final Random random = new Random();
	private List<HostAndPort> slaves;
    private int timeout;
	private String password;

	public RandomJedisFactory(List<HostAndPort> slaves, String password, int timeout) {
		this.slaves = slaves;
		this.timeout = timeout;
		this.password = password;
	}

	@Override
	public PooledObject<Jedis> makeObject() throws Exception {
		HostAndPort slaveInfo = null;
		Jedis jedis = null;
		slaveInfo = slaves.get(random.nextInt(slaves.size()));
		jedis = new Jedis(slaveInfo.host,slaveInfo.port,timeout);
		if (null != this.password) {
			jedis.auth(this.password);
		}
		PooledObject<Jedis> pooledObject = new DefaultPooledObject<Jedis>(jedis);
		return pooledObject;
	}

	@Override
	public void destroyObject(PooledObject<Jedis> p) throws Exception {
		Jedis jedis = p.getObject();
		try {
			jedis.quit();
		} catch (Exception e) {} 
		finally {
			jedis.disconnect();
		}
	}

	@Override
	public boolean validateObject(PooledObject<Jedis> p) {
		Jedis jedis = p.getObject();
		try {
			if (!jedis.ping().equals("PONG")) {
				return false;
			}
			return true;
		} catch (Exception e) {
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
