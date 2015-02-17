package name.zicat.relax.nedis.sentinel;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public abstract class JedisPool<T extends Jedis> {

	protected volatile ObjectPool<T> internalPool;

	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private WriteLock writeLock = lock.writeLock();
	private ReadLock readLock = lock.readLock();

	/**
	 * Using this constructor means you have to set and initialize the
	 * internalPool yourself.
	 */
	public JedisPool() {

	}

	public JedisPool(final GenericObjectPoolConfig poolConfig,
			PooledObjectFactory<T> factory) {
		initPool(poolConfig, factory);
	}

	public void initPool(final GenericObjectPoolConfig poolConfig,
			PooledObjectFactory<T> factory) {
		writeLock.lock();
		try {
			if (internalPool != null) {
				try {
					internalPool.close();
				} catch (Exception e) {
				}
			}
			this.internalPool = new GenericObjectPool<T>(factory, poolConfig);
		} catch (Exception e) {
		} finally {
			writeLock.unlock();
		}
	}

	public T getResource() {
		T jedis = null;
		readLock.lock();
		try {
			jedis = internalPool.borrowObject();
		} catch (Throwable e) {
			throw new JedisConnectionException(
					"Could not get a resource from the pool " + e);
		} finally {
			readLock.unlock();
		}
		return jedis;
	}

	public void returnResourceObject(final T resource) {
		try {
			if(resource == null) {
				return;
			}
			if(internalPool == null) {
				release(resource);
			}
			internalPool.returnObject(resource);
		} catch (Exception e) {
			release(resource);
		} 
	}

	public void returnBrokenResource(final T resource) {
		returnBrokenResourceObject(resource);
	}

	public void returnResource(final T resource) {
		returnResourceObject(resource);
	}

	protected void returnBrokenResourceObject(final T resource) {
		try {
			if(resource == null) {
				return;
			}
			if(internalPool == null) {
				release(resource);
				return;
			}
			internalPool.invalidateObject(resource);
		} catch (Exception e) {
			release(resource);
		}
	}

	public void destroy() {
		try {
			internalPool.close();
		} catch (Exception e) {
			throw new JedisException("Could not destroy the pool", e);
		}
	}

	private void release(T resource) {
		if (resource != null) {
			try {
				resource.quit();
			} catch (Throwable e1) {
			} finally {
				try {
					resource.disconnect();
				} catch (Throwable e) {
				} finally {
					resource = null;
				}
			}
		}
	}
	
	public int poolSize() {
		return this.internalPool.getNumIdle();
	}
}
