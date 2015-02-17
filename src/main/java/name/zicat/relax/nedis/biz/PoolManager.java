package name.zicat.relax.nedis.biz;

import java.util.HashMap;
import java.util.Map;

import name.zicat.relax.nedis.config.ConfigHolder;
import name.zicat.relax.nedis.config.NedisConfig;
import name.zicat.relax.nedis.sentinel.JedisPool;
import name.zicat.relax.nedis.sentinel.JedisSentinelMasterPool;
import name.zicat.relax.nedis.sentinel.JedisSentinelSlavePool;
import name.zicat.relax.nedis.utils.NedisException;
import redis.clients.jedis.Jedis;

/**
 * @company Newegg Tech (China) Co, Ltd
 * @author lz31
 * @date 2014-12-18
 *
 */
public class PoolManager {
	
	private final Map<String, PoolTuple> poolManagerMap = new HashMap<String, PoolTuple>();
	
	private static final PoolManager poolManager = new PoolManager();
	
	/**
	 * 
	 * @return
	 */
	protected Map<String, PoolTuple> getAllPoolTuple() {
		return poolManagerMap;
	}
	
	/**
	 * 
	 * @return
	 */
	public static final PoolManager getInstance() {
		return poolManager;
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	public PoolTuple getPoolTuple(String key) {
		
		if(key == null) {
			throw new NedisException("param key is null when invoke getPoolTuple");
		}
		
		PoolTuple poolTuple = poolManagerMap.get(key);
		if(poolTuple == null) {
			poolTuple = init(key);
		}
		return poolTuple;
		
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	private synchronized PoolTuple init(String key) {
		PoolTuple poolTuple = poolManagerMap.get(key);
		if(poolTuple == null) {
			NedisConfig nedisConfig = ConfigHolder.getNedisConfig(key);
			JedisPool<Jedis> masterPool = new JedisSentinelMasterPool(nedisConfig.getSentinelsConfig().getMasterName(), nedisConfig.getSentinelsConfig().getSentinelInfo(), nedisConfig.getMasterConfig(), nedisConfig.getSentinelsConfig().getSoTimeout());
			JedisPool<Jedis> slavePool = new JedisSentinelSlavePool(nedisConfig.getSentinelsConfig().getMasterName(), nedisConfig.getSentinelsConfig().getSentinelInfo(), nedisConfig.getSentinelsConfig().getReflashWaitTimeMillis(), nedisConfig.getSlaveConfig(), nedisConfig.getSentinelsConfig().getSoTimeout());
			poolTuple = new PoolTuple(masterPool, slavePool, nedisConfig.getSentinelsConfig().isSlaveBackUp());
			poolManagerMap.put(key, poolTuple);
		}
		return poolTuple;
	}
	
	/**
	 * 
	 */
	private PoolManager() {
		//nothing to do
	}
}
