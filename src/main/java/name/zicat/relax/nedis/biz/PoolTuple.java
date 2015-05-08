package name.zicat.relax.nedis.biz;

import name.zicat.relax.nedis.sentinel.JedisPool;
import name.zicat.relax.nedis.utils.NedisException;
import redis.clients.jedis.Jedis;


/**
 * 
 * @company Newegg Tech (China) Co, Ltd
 * @author lz31
 * @date 2014-12-18
 * 
 */
public class PoolTuple {
	
	private JedisPool<Jedis> firstPool;
	private JedisPool<Jedis> lastPool;
	private boolean isSlaveBackUp = false;
	
	public PoolTuple(JedisPool<Jedis> firstPool, JedisPool<Jedis> lastPool, boolean isSlaveBackUp) {
		
		if(firstPool == null) {
			throw new NedisException("param firstPool is null when structure PoolTuple object");
		}
		
		if(lastPool == null) {
			throw new NedisException("param lastPool is null when structure PoolTuple object");
		}
		
		this.firstPool = firstPool;
		this.lastPool = lastPool;
		this.isSlaveBackUp = isSlaveBackUp;
		
	}
	
	public JedisPool<Jedis> getFirst() {
		return firstPool;
	}
	
	public JedisPool<Jedis> getLast() {
		return lastPool;
	}
	
	public boolean isSlaveBackUp() {
		return isSlaveBackUp;
	}
	
	public void destoryAll() {
		try {
			firstPool.destroy();
		} catch(Throwable e) {
			//nothing to do
		} finally {
			firstPool = null;
			try {
				lastPool.destroy();
			} catch(Throwable e){
				//nothing to do
			} finally {
				lastPool = null;
			}
		}
	}
}
