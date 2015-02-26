package name.zicat.relax.nedis.biz;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import name.zicat.relax.nedis.config.ConfigHolder;
import name.zicat.relax.nedis.sentinel.JedisPool;
import name.zicat.relax.nedis.utils.NedisException;
import name.zicat.relax.nedis.utils.RedisExecException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

/**
 *
 * @company Newegg Tech (China) Co, Ltd
 * @author lz31
 * @date 2014-2-26
 *
 */
public class Nedis {

    private JedisPool<Jedis> jedisSentinelMasterPool;

    private JedisPool<Jedis> jedisSentinelSlavesPool;

    private Jedis slaveJedis;
    private Jedis masterJedis;
    private Mode mode = Mode.ReadWrite;

    private boolean isSlaveBackUp = false;

    private final String RETURN_OK = "OK";

    /**
     * init Jedis object of master and slave
     */
    public Nedis() {
        this(ConfigHolder.DEFAULT_FILE_NAME);
    }

    public Nedis(String fileName) {
        PoolTuple poolTuple = PoolManager.getInstance().getPoolTuple(fileName);
        jedisSentinelMasterPool = poolTuple.getFirst();
        jedisSentinelSlavesPool = poolTuple.getLast();
        isSlaveBackUp = poolTuple.isSlaveBackUp();
        getResource();
    }

    public Nedis(Mode mode) {
        this(mode, ConfigHolder.DEFAULT_FILE_NAME);
    }

    public Nedis(Mode mode, String fileName) {
        this.mode = mode;
        PoolTuple poolTuple = PoolManager.getInstance().getPoolTuple(fileName);
        jedisSentinelMasterPool = poolTuple.getFirst();
        jedisSentinelSlavesPool = poolTuple.getLast();
        isSlaveBackUp = poolTuple.isSlaveBackUp();
        getResource();
    }
    /**
     * get jedis object of master and slave
     * @throws Exception
     */
    public void getResource() throws NedisException {

        if(masterJedis != null) {
            jedisSentinelMasterPool.returnResource(masterJedis);
        }

        if(slaveJedis != null) {
            jedisSentinelSlavesPool.returnResource(slaveJedis);
        }
        try {
            if(isSlaveBackUp) {
                masterJedis = jedisSentinelMasterPool.getResource();
            } else if(mode != Mode.ReadOnly) {
                masterJedis = jedisSentinelMasterPool.getResource();
            }
        } catch (Exception e) {
            throw new NedisException("get masterJedis fail "+e);
        }

        try {
            if(mode != Mode.WriteOnly) {
                slaveJedis = jedisSentinelSlavesPool.getResource();
            }
        } catch (Exception e) {
            if(masterJedis != null) {
                jedisSentinelMasterPool.returnResource(masterJedis);
                masterJedis = null;
            }
            throw new NedisException("get slaveJedis fail "+e);
        }

    }



    /**
     * receive Jedis object of master and slave
     */
    public void returnResource() {

        if(masterJedis != null){
            try {
                jedisSentinelMasterPool.returnResource(masterJedis);
            } catch(Throwable e){}
            finally {
                masterJedis = null;
            }
        }
        if(slaveJedis != null){
            try {
                jedisSentinelSlavesPool.returnResource(slaveJedis);
            }catch(Throwable e){}
            finally{
                slaveJedis = null;
            }
        }
    }

    /**
     * receive jedisSentinelMasterPool and jedisSentinelSlavesPool
     */
    public static void destroyPool() {

        Map<String, PoolTuple> poolManagerMap = PoolManager.getInstance().getAllPoolTuple();
        for(Entry<String, PoolTuple> poolManagerEntry: poolManagerMap.entrySet()) {
            poolManagerEntry.getValue().destoryAll();
        }

        poolManagerMap.clear();
        poolManagerMap = null;

    }

    public static interface ReadPipelineHandlerListener<T> {
        public T callback(Pipeline pipeline);
    }

    public static interface WritePipelineHandlerListener<T> {
        public T callback(Pipeline pipeline);
    }

    public static interface ReadHandlerListener<T> {
        public T callback(Jedis jedis);
    }

    public static interface WriteHandlerListener<T> {
        public T callback(Jedis jedis);
    }

    /**
     *
     * @param listener
     * @return
     */
    public <T> T handle(ReadPipelineHandlerListener<T> listener) {

        try{

            checkMode(Mode.ReadOnly);
            Pipeline pipeline = null;
            if(isSlaveBackUp) {
                pipeline = masterJedis.pipelined();
            } else {
                pipeline = slaveJedis.pipelined();
            }
            return listener.callback(pipeline);
        }catch(Throwable e){

            if(isSlaveBackUp) {
                dealMasterException();
            } else {
                dealSlaveException();
            }
            throw new NedisException(e);
        }
    }

    /**
     *
     * @param listener
     * @return
     */
    public <T> T handle(ReadHandlerListener<T> listener) {
        try{

            checkMode(Mode.ReadOnly);
            if(isSlaveBackUp) {
                return listener.callback(masterJedis);
            } else {
                return listener.callback(slaveJedis);
            }
        }catch(Throwable e){

            if(isSlaveBackUp) {
                dealMasterException();
            } else {
                dealSlaveException();
            }
            throw new NedisException(e);
        }
    }

    /**
     *
     * @param listener
     * @return
     */
    public <T> T handle(WritePipelineHandlerListener<T> listener) {

        try {

            checkMode(Mode.WriteOnly);
            Pipeline pipeline = masterJedis.pipelined();
            return listener.callback(pipeline);
        } catch (Throwable e) {

            dealMasterException();
            throw new NedisException(e);
        }
    }

    /**
     *
     * @param listener
     * @return
     */
    public <T> T handle(WriteHandlerListener<T> listener) {

        try {

            checkMode(Mode.WriteOnly);
            return listener.callback(masterJedis);
        } catch (Throwable e) {

            dealMasterException();
            throw new NedisException(e);
        }
    }

    /**
     * get string value from redis by index and key
     * @param index
     * @param key
     * @return
     */
    public String getString(final Integer index, final String key) {

        return handle(new ReadPipelineHandlerListener<String>() {

            @Override
            public String callback(Pipeline pipeline) {

                pipeline.select(index);
                Response<String> value = pipeline.get(key);
                pipeline.sync();
                return value.get();
            }
        });
    }

    /**
     * get list string value from redis by index and keys
     * @param dbIndex
     * @param keys
     * @return
     */
    public List<String> getStringList(final Integer dbIndex, final String... keys) {

        return handle(new ReadPipelineHandlerListener<List<String>>() {

            @Override
            public List<String> callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                Response<List<String>> res = pipeline.mget(keys);
                pipeline.sync();
                return res.get();
            }
        });
    }


    /**
     * get string value from redis by index and key
     *
     * @param index
     * @param key
     * @returnd
     */
    public byte[] getBytes(final Integer index, final byte[] key) {

        return handle(new ReadPipelineHandlerListener<byte[]>() {

            @Override
            public byte[] callback(Pipeline pipeline) {

                pipeline.select(index);
                Response<byte[]> res = pipeline.get(key);
                pipeline.sync();
                return res.get();
            }
        });
    }

    /**
     * get list string value from redis by index and keys use pipeline
     *
     * @param dbIndex
     * @param keys
     * @return
     */
    public Map<byte[], byte[]> getBytesBatch(final Integer dbIndex, final List<byte[]> keys) {

        return handle(new ReadPipelineHandlerListener<Map<byte[], byte[]>>() {

            @Override
            public Map<byte[], byte[]> callback(Pipeline pipeline) {

                Map<byte[], byte[]> values = new HashMap<byte[], byte[]>();
                Map<byte[], Response<byte[]>> mapReponse = new HashMap<byte[], Response<byte[]>>();

                pipeline.select(dbIndex);

                for (byte[] key : keys) {
                    mapReponse.put(key, pipeline.get(key));
                }
                pipeline.sync();

                for (Entry<byte[], Response<byte[]>> entry : mapReponse.entrySet()) {
                    values.put(entry.getKey(), entry.getValue().get());
                }

                return values;
            }
        });
    }

    /**
     * get map value from reids vy index, keys and fieldName
     * @param dbIndex
     * @param key
     * @param fieldName
     * @return
     */
    public String getHashMapVal(final Integer dbIndex, final String key, final String fieldName) {

        return handle(new ReadPipelineHandlerListener<String>() {

            @Override
            public String callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                Response<String> res = pipeline.hget(key, fieldName);
                pipeline.sync();
                return res.get();
            }
        });
    }


    /**
     * get list string value from redis by index and keys
     * use pipeline
     * @param dbIndex
     * @param keys
     * @return
     */
    public Map<String, String> getStringBatch(final Integer dbIndex, final String[] keys) {

        return handle(new ReadPipelineHandlerListener<Map<String, String>>() {

            @Override
            public Map<String, String> callback(Pipeline pipeline) {

                pipeline.select(dbIndex);

                Map<String, String> values=new HashMap<String, String>();
                Map<String, Response<String>> mapReponse=new HashMap<String, Response<String>>();

                for (String key:keys) {
                    mapReponse.put(key, pipeline.get(key));
                }
                pipeline.sync();

                for(Entry<String, Response<String>> entry: mapReponse.entrySet()) {
                    values.put(entry.getKey(), entry.getValue().get());
                }

                return values;
            }
        });

    }

    /**
     * get list string value from redis by index and keys
     * use pipeline
     * @param dbIndex
     * @param keys
     * @return
     */
    public Map<String, String>  getStringBatch(Integer dbIndex, List<String> keys) {
        return getStringBatch(dbIndex, keys.toArray(new String[]{}));
    }

    /**
     * get all List value from redis by index and key
     * @param dbIndex
     * @param key
     * @return
     */
    public List<String> getList(Integer dbIndex, String key){
        return getListRange(dbIndex, key, 0, -1);
    }

    /**
     * get range list value from redis by index, key ,startIndex and endIndex
     * @param dbIndex
     * @param key
     * @param startIndex
     * @param endIndex
     * @return
     */
    public List<String> getListRange(final Integer dbIndex, final String key, final long startIndex, final long endIndex) {

        return handle(new ReadPipelineHandlerListener<List<String>>() {

            @Override
            public List<String> callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                Response<List<String>>res = pipeline.lrange(key, startIndex, endIndex);
                pipeline.sync();
                return res.get();
            }
        });
    }

    /**
     * get all List value from redis by index and key
     * use pipeline
     * @param dbIndex
     * @param key
     * @return
     */
    public Map<String, List<String>> getListBatch(final Integer dbIndex, final List<String> keys) {

        return handle(new ReadPipelineHandlerListener<Map<String, List<String>>>() {

            @Override
            public Map<String, List<String>> callback(Pipeline pipeline) {

                Map<String, List<String>> values = new HashMap<String, List<String>>();
                Map<String, Response<List<String>>> mapResponse = new HashMap<String, Response<List<String>>>();

                pipeline.select(dbIndex);

                for(String key: keys) {
                    mapResponse.put(key, pipeline.lrange(key, 0, -1));
                }
                pipeline.sync();

                for(Entry<String,Response<List<String>>> entry: mapResponse.entrySet()) {
                    values.put(entry.getKey(), entry.getValue().get());
                }

                return values;
            }
        });
    }

    /**
     * get set value from redis by index and key
     * use pipeline
     * @param dbIndex
     * @param key
     * @return
     */
    public Set<String> getSet(final Integer dbIndex, final String key) {

        return handle(new ReadPipelineHandlerListener<Set<String>>() {

            @Override
            public Set<String> callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                Response<Set<String>> res = pipeline.smembers(key);
                pipeline.sync();
                return res.get();
            }
        });
    }

    /**
     * get set value from redis by index and key
     * use pipeline
     * use pipeline
     * @param dbIndex
     * @param key
     * @return
     */
    public Map<String, Set<String>> getSetBatch(final Integer dbIndex, final List<String> keys) {

        return handle(new ReadPipelineHandlerListener<Map<String, Set<String>>>() {

            @Override
            public Map<String, Set<String>> callback(Pipeline pipeline) {

                Map<String, Set<String>> values = new HashMap<String, Set<String>>();
                Map<String, Response<Set<String>>> mapResponse = new HashMap<String, Response<Set<String>>>();

                pipeline.select(dbIndex);

                for(String key: keys) {
                    mapResponse.put(key, pipeline.smembers(key));
                }
                pipeline.sync();

                for(Entry<String,Response<Set<String>>> entry: mapResponse.entrySet()) {
                    values.put(entry.getKey(), entry.getValue().get());
                }

                return values;
            }
        });
    }

    /**
     * get map value from redis by index and key
     * @param dbIndex
     * @param key
     * @return
     */
    public Map<String, String> getHashMap(final Integer dbIndex, final String key) {

        return handle(new ReadPipelineHandlerListener<Map<String, String>>() {

            @Override
            public Map<String, String> callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                Response<Map<String, String>> res = pipeline.hgetAll(key);
                pipeline.sync();
                return res.get();
            }
        });
    }


    /**
     * get map value from redis by index and key
     * use pipeline
     * @param dbIndex
     * @param key
     * @return
     */
    public Map<String, Map<String, String>> getHashMapBatch(final Integer dbIndex, final List<String> keys) {

        return handle(new ReadPipelineHandlerListener<Map<String, Map<String, String>>>() {

            @Override
            public Map<String, Map<String, String>> callback(Pipeline pipeline) {

                Map<String, Map<String, String>> values = new HashMap<String, Map<String, String>>();
                Map<String, Response<Map<String, String>>> mapResponse = new HashMap<String, Response<Map<String, String>>>();

                pipeline.select(dbIndex);
                for(String str: keys) {
                    mapResponse.put(str, pipeline.hgetAll(str));
                }

                pipeline.sync();

                for(Entry<String, Response<Map<String, String>>> entry : mapResponse.entrySet()) {
                    values.put(entry.getKey(), entry.getValue().get());
                }

                return values;
            }
        });
    }

    @Deprecated
    public Set<String> getAllKeysByString(final Integer dbIndex) {
        return getKeysByStringWithPattern(dbIndex, "*:*");
    }

    /**
     *
     * @param dbIndex
     * @param pattern
     * @return
     */
    public Set<String> getKeysByStringWithPattern(final Integer dbIndex, final String pattern) {

        return handle(new ReadPipelineHandlerListener<Set<String>>() {

            @Override
            public Set<String> callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                Response<Set<String>> res = pipeline.keys(pattern);
                pipeline.sync();
                return res.get();
            }
        });
    }

    /**
     * wheter contains the key in dbIndex from redis
     * @param dbIndex
     * @param key
     * @return
     */
    public boolean contains(final Integer dbIndex, final String key) {

        return handle(new ReadPipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                Response<Boolean> res = pipeline.exists(key);
                pipeline.sync();
                return res.get();
            }
        });
    }

    /**
     * wheter contains the key in dbIndex from redis
     *
     * @param dbIndex
     * @param key
     * @return
     */
    public boolean contains(final Integer dbIndex, final byte[] key) {

        return handle(new ReadPipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                Response<Boolean> res = pipeline.exists(key);
                pipeline.sync();
                return res.get();
            }
        });
    }

    /**
     * remove keys in dbIndex from redis
     * @param dbIndex
     * @param key
     * @return
     */
    public boolean remove(final Integer dbIndex, final String ...key) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                Response<Long> res = pipeline.del(key);
                pipeline.sync();
                return res.get() > 0 ;
            }
        });
    }

    /**
     * random key in dbIndex from redis
     * @param dbIndex
     * @return
     */
    public String randomKey(final Integer dbIndex) {

        return handle(new ReadPipelineHandlerListener<String>() {

            @Override
            public String callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                Response<String> res = pipeline.randomKey();
                pipeline.sync();
                return res.get();
            }
        });
    }

    /************************add******************************/

    /**
     * add key and value to dbIndex (type=string)
     * @param dbIndex
     * @param key
     * @param value
     * @return
     */
    public boolean addString(final Integer dbIndex, final String key, final String value) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                pipeline.set(key, value);
                pipeline.sync();
                return true;
            }
        });
    }

    /**
     * add key and value to dbIndex (type=string)
     * @param dbIndex
     * @param key
     * @param value
     * @param seconds
     * @return
     */
    public boolean addString(final Integer dbIndex, final String key, final String value, final int seconds) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                pipeline.set(key, value);
                pipeline.expire(key, seconds);
                pipeline.sync();
                return true;
            }
        });
    }

    /**
     * add keys and values to dbIndex (type=String)
     * use pipeline
     * @param dbIndex
     * @param batchDatas
     * @return
     */
    public boolean addStringBach(final Integer dbIndex, final Map<String, String> batchDatas) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                for(Entry<String, String> entry: batchDatas.entrySet()) {
                    pipeline.set(entry.getKey(), entry.getValue());
                }

                pipeline.sync();
                return true;
            }
        });
    }

    /**
     * add keys and values to dbIndex (type=String)
     * use pipeline
     * @param dbIndex
     * @param batchDatas
     * @param seconds
     * @return
     */
    public boolean addStringBach(final Integer dbIndex, final Map<String, String> batchDatas, final int seconds) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                for(Entry<String, String> entry: batchDatas.entrySet()) {
                    pipeline.set(entry.getKey(), entry.getValue());
                    pipeline.expire(entry.getKey(), seconds);
                }

                pipeline.sync();
                return true;
            }
        });
    }

    /**
     * add keys and values to dbIndex (type=byte) use pipeline
     *
     * @param dbIndex
     * @param batchDatas
     * @return
     */
    public boolean addbyteBach(final Integer dbIndex, final Map<byte[], byte[]> batchDatas) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                for (Entry<byte[], byte[]> entry : batchDatas.entrySet()) {
                    pipeline.set(entry.getKey(), entry.getValue());
                }

                pipeline.sync();
                return true;
            }
        });
    }

    /**
     * add keys and values to dbIndex (type=byte) use pipeline
     *
     * @param dbIndex
     * @param batchDatas
     * @param second
     * @return
     */
    public boolean addbyteBach(final Integer dbIndex, final Map<byte[], byte[]> batchDatas, final int seconds) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                for (Entry<byte[], byte[]> entry : batchDatas.entrySet()) {
                    pipeline.set(entry.getKey(), entry.getValue());
                    pipeline.expire(entry.getKey(), seconds);
                }

                pipeline.sync();
                return true;
            }
        });
    }

    /**
     * add keys and values to dbIndex (type=byte) use pipeline
     *
     * @param dbIndex
     * @param batchDatas
     * @return
     */
    public boolean addbyteBachWithTTL(final Integer dbIndex, final Map<byte[], byte[]> batchDatas, final int ttlSeconds) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                for (Entry<byte[], byte[]> entry : batchDatas.entrySet()) {
                    pipeline.setex(entry.getKey(),ttlSeconds,entry.getValue());
                }

                pipeline.sync();

                return true;
            }
        });
    }

    /**
     * zadd
     *
     * @param dbIndex
     * @param batchDatas
     * @return
     */
    public boolean zadd(final Integer dbIndex, final String key, final double score, final String member) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                pipeline.zadd(key, score, member);
                pipeline.sync();
                return true;
            }
        });
    }


    /**
     * zrevrangeByScore
     *
     * @param dbIndex
     * @param batchDatas
     * @return
     */
    public Set<String> zrevrangeByScore(final Integer dbIndex, final String key, final double max, final double min) {

        return handle(new ReadPipelineHandlerListener<Set<String>>() {

            @Override
            public Set<String> callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                Response<Set<String>>  response = pipeline.zrevrangeByScore(key, max, min);
                pipeline.sync();
                return response.get();
            }
        });
    }

    /**
     * zrevrangeByScore
     *
     * @param dbIndex
     * @param batchDatas
     * @return
     */
    public Set<String> zrangeByScore(final Integer dbIndex, final String key, final double max, final double min) {

        return handle(new ReadPipelineHandlerListener<Set<String>>() {

            @Override
            public Set<String> callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                Response<Set<String>>  response = pipeline.zrangeByScore(key,min,max);
                pipeline.sync();
                return response.get();
            }
        });
    }


    /**
     * zrevrangeByScore
     *
     * @param dbIndex
     * @param batchDatas
     * @return
     */
    public Set<Tuple> zrangeByScoreWithScores(final Integer dbIndex, final String key, final double max, final double min) {

        return handle(new ReadPipelineHandlerListener<Set<Tuple>>() {

            @Override
            public Set<Tuple> callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                Response<Set<Tuple>> response = pipeline.zrangeByScoreWithScores(key, min, max);
                pipeline.sync();
                return response.get();
            }
        });
    }

    /**
     * zremrangeByScore
     * return delete nums
     *
     * @param dbIndex
     * @param batchDatas
     * @return
     */
    public Long zremrangeByScore(final Integer dbIndex, final String key, final double start, final double end) {

        return handle(new ReadPipelineHandlerListener<Long>() {

            @Override
            public Long callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                Response<Long>  response = pipeline.zremrangeByScore(key, start, end);
                pipeline.sync();
                return response.get();
            }
        });
    }


    /**
     *
     * scan from master, because of slave is multi , master is single
     *
     * @param cursor
     * @param params
     * @return
     */
    public ScanResult<String> scan(final int index ,final String cursor, final ScanParams params) {

        return handle(new WriteHandlerListener<ScanResult<String>>() {

            @Override
            public ScanResult<String> callback(Jedis jedis) {
                jedis.select(index);
                return jedis.scan(cursor, params);
            }
        });
    }

    /**
     * add key and value to dbIndex (type=byte[])
     *
     * @param dbIndex
     * @param key
     * @param value
     * @return
     */
    public boolean addBytes(final Integer dbIndex, final byte[] key, final byte[] value) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                pipeline.set(key, value);
                pipeline.sync();
                return true;
            }
        });
    }

    /**
     * add key and value to dbIndex (type=byte[]) with multi transaction
     *
     * @param dbIndex
     * @param key
     * @param value
     * @return
     */
    public boolean addBytesWithTransaction(final Integer dbIndex, final byte[] key, final byte[] value) {

        return handle(new WriteHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Jedis jedis) {

                Transaction transaction = jedis.multi();
                transaction.select(dbIndex);
                transaction.set(key, value);
                checkReturnOK(transaction.exec());
                return true;
            }
        });
    }

    /**
     * add key and value to dbIndex (type=byte[]) with multi transaction
     *
     * @param dbIndex
     * @param key
     * @param value
     * @return
     */
    public boolean addBytesWithTransactionWithTTL(final Integer dbIndex, final byte[] key, final byte[] value, final int ttlSeconds) {

        return handle(new WriteHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Jedis jedis) {

                Transaction transaction = masterJedis.multi();
                transaction.select(dbIndex);
                transaction.setex(key, ttlSeconds, value);
                checkReturnOK(transaction.exec());
                return true;
            }
        });
    }


    /**
     * add key and value to dbIndex (type=byte[]) with multi transaction
     *
     * @param dbIndex
     * @param key
     * @param value
     * @return
     */
    public boolean addStringWithTransaction(final Integer dbIndex, final String key, final String value) {

        return handle(new WriteHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Jedis jedis) {

                Transaction transaction = masterJedis.multi();
                transaction.select(dbIndex);
                transaction.set(key, value);
                checkReturnOK(transaction.exec());
                return true;
            }
        });
    }

    /**
     *
     * @param dbIndex
     * @param keys
     */
    public void watchKeys(Integer dbIndex, final byte[]... keys) {
        try {
            masterJedis.select(dbIndex);
            checkReturnOK(masterJedis.watch(keys));
        } catch (RedisExecException e) {
            throw e;
        } catch (Throwable e) {
            dealMasterException();
            throw new NedisException(e);
        }
    }

    /**
     *
     * @param dbIndex
     */
    public void unWatchKeys(Integer dbIndex) {
        try {
            masterJedis.select(dbIndex);
            checkReturnOK(masterJedis.unwatch());
        } catch (RedisExecException e) {
            throw new NedisException(e.getMessage());
        } catch (Throwable e) {
            dealMasterException();
            throw new NedisException(e);
        }
    }

    /**
     *
     * @param list
     */
    private void checkReturnOK(List<Object> list) {
        if (null != list && list.size() > 0) {
            String returnValue = (String) list.get(0);
            if (returnValue.equalsIgnoreCase(RETURN_OK)) {
                return;
            }
        }
        throw new RedisExecException("return is error , list = " + list);
    }

    /**
     *
     * @param returnValue
     */
    private void checkReturnOK(String returnValue) {
        if (null != returnValue && returnValue.equalsIgnoreCase(RETURN_OK)) {
            return;
        }
        throw new RedisExecException("return is error , returnValue = " + returnValue);
    }

    /**
     * add key and values to dbIndex (type=List)
     * @param dbIndex
     * @param batchDatas
     * @return
     */
    public boolean addList2Nedis(final Integer dbIndex, final String key, final List<String> list, final boolean isRemoveOld) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                if(isRemoveOld) {
                    pipeline.del(key);
                }
                pipeline.rpush(key, list.toArray(new String[]{}));
                pipeline.sync();
                return true;
            }
        });
    }

    /**
     * add key and values to dbIndex (type=List)
     * @param dbIndex
     * @param batchDatas
     * @param seconds
     * @return
     */
    public boolean addList2Nedis(final Integer dbIndex, final String key, final List<String> list, final boolean isRemoveOld, final int seconds) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                if(isRemoveOld) {
                    pipeline.del(key);
                }
                pipeline.rpush(key, list.toArray(new String[]{}));
                pipeline.expire(key, seconds);
                pipeline.sync();
                return true;
            }
        });
    }

    /**
     * add keys and values to dbIndex (type=List)
     * use pipeline
     * @param dbIndex
     * @param batchDatas
     * @return
     */
    public boolean addList2NedisBatch(final Integer dbIndex, final Map<String, List<String>> batchDatas, final boolean isRemoveOld) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                for(Entry<String, List<String>> entry: batchDatas.entrySet()) {

                    if(isRemoveOld) {
                        pipeline.del(entry.getKey());
                    }

                    pipeline.rpush(entry.getKey(), entry.getValue().toArray(new String[]{}));
                }

                pipeline.sync();

                return true;
            }
        });
    }

    /**
     * add keys and values to dbIndex (type=List)
     * use pipeline
     * @param dbIndex
     * @param batchDatas
     * @param isRemoveOld
     * @param seconds
     * @return
     */
    public boolean addList2NedisBatch(final Integer dbIndex, final Map<String, List<String>> batchDatas, final boolean isRemoveOld, final int seconds) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                for(Entry<String, List<String>> entry: batchDatas.entrySet()) {

                    if(isRemoveOld) {
                        pipeline.del(entry.getKey());
                    }

                    pipeline.rpush(entry.getKey(), entry.getValue().toArray(new String[]{}));
                    pipeline.expire(entry.getKey(), seconds);
                }

                pipeline.sync();

                return true;
            }
        });
    }

    /**
     *
     * @return
     */
    public String info() {

        return handle(new WritePipelineHandlerListener<String>() {

            @Override
            public String callback(Pipeline pipeline) {
                Response<String> info = pipeline.info();
                pipeline.sync();
                return info.get();
            }
        });
    }

    /**
     *
     * @param section
     * @return
     */
    public String info(final String section) {

        return handle(new WriteHandlerListener<String>() {

            @Override
            public String callback(Jedis jedis) {
                return jedis.info(section);
            }
        });
    }

    /**
     * add key and values to dbIndex (type=Set)
     * @param dbIndex
     * @param batchDatas
     * @return
     */
    public boolean addSet2Nedis(final Integer dbIndex, final String key, final Set<String> set, final boolean isRemoveOld) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {
                pipeline.select(dbIndex);

                if(isRemoveOld) {
                    pipeline.del(key);
                }

                pipeline.sadd(key, set.toArray(new String[]{}));

                pipeline.sync();

                return true;
            }
        });
    }

    /**
     * add key and values to dbIndex (type=Set)
     * @param dbIndex
     * @param batchDatas
     * @param seconds
     * @return
     */
    public boolean addSet2Nedis(final Integer dbIndex, final String key, final Set<String> set, final boolean isRemoveOld, final int seconds) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);

                if(isRemoveOld) {
                    pipeline.del(key);
                }

                pipeline.sadd(key, set.toArray(new String[]{}));
                pipeline.expire(key, seconds);
                pipeline.sync();

                return true;
            }
        });
    }

    /**
     * add keys and values to dbIndex (type=Set)
     * use pipeline
     * @param dbIndex
     * @param batchDatas
     * @return
     */
    public boolean addSet2NedisBatch(final Integer dbIndex, final Map<String, Set<String>> batchDatas, final boolean isRemoveOld) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);

                for(Entry<String, Set<String>> entry: batchDatas.entrySet()) {

                    if(isRemoveOld) {
                        pipeline.del(entry.getKey());
                    }

                    pipeline.sadd(entry.getKey(), entry.getValue().toArray(new String[]{}));
                }

                pipeline.sync();

                return true;
            }
        });
    }


    /**
     * add keys and values to dbIndex (type=Set)
     * use pipeline
     * @param dbIndex
     * @param batchDatas
     * @param seconds
     * @return
     */
    public boolean addSet2NedisBatch(final Integer dbIndex, final Map<String, Set<String>> batchDatas, final boolean isRemoveOld, final int seconds) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                for(Entry<String, Set<String>> entry: batchDatas.entrySet()) {

                    if(isRemoveOld) {
                        pipeline.del(entry.getKey());
                    }

                    pipeline.sadd(entry.getKey(), entry.getValue().toArray(new String[]{}));
                    pipeline.expire(entry.getKey(), seconds);
                }

                pipeline.sync();

                return true;
            }
        });
    }

    /**
     * add key and map value to dbIndex (type=Map)
     * @param dbIndex
     * @param batchDatas
     * @return
     */
    public boolean addHashMap2Nedis(final Integer dbIndex, final String key, final String field, final String value, final boolean isRemoveOld) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);

                if(isRemoveOld) {
                    pipeline.del(key);
                }

                pipeline.hset(key, field, value);
                pipeline.sync();
                return true;
            }
        });
    }

    /**
     * add key and map value to dbIndex (type=Map)
     * @param dbIndex
     * @param batchDatas
     * @param seconds
     * @return
     */
    public boolean addHashMap2Nedis(final Integer dbIndex, final String key, final String field, final String value, final boolean isRemoveOld, final int seconds) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);
                if(isRemoveOld) {
                    pipeline.del(key);
                }

                pipeline.hset(key, field, value);
                pipeline.expire(key, seconds);
                pipeline.sync();
                return true;
            }
        });
    }

    /**
     * add keys and map values to dbIndex (type=Map)
     * use pipeline
     * @param dbIndex
     * @param batchDatas
     * @return
     */
    public boolean addHashMap2NedisBatch(final Integer dbIndex, final Map<String, Map<String, String>> batchDatas, final boolean isRemoveOld) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);

                for(Entry<String, Map<String, String>> entry : batchDatas.entrySet()) {

                    if(isRemoveOld) {
                        pipeline.del(entry.getKey());
                    }

                    pipeline.hmset(entry.getKey(), entry.getValue());
                }

                pipeline.sync();

                return true;
            }
        });
    }

    /**
     * add keys and map values to dbIndex (type=Map)
     * use pipeline
     * @param dbIndex
     * @param batchDatas
     * @param seconds
     * @return
     */
    public boolean addHashMap2NedisBatch(final Integer dbIndex, final Map<String, Map<String, String>> batchDatas, final boolean isRemoveOld, final int seconds) {

        return handle(new WritePipelineHandlerListener<Boolean>() {

            @Override
            public Boolean callback(Pipeline pipeline) {

                pipeline.select(dbIndex);

                for(Entry<String, Map<String, String>> entry : batchDatas.entrySet()) {

                    if(isRemoveOld) {
                        pipeline.del(entry.getKey());
                    }

                    pipeline.hmset(entry.getKey(), entry.getValue());
                    pipeline.expire(entry.getKey(), seconds);
                }

                pipeline.sync();
                return true;
            }
        });
    }

    /**
     * the function to deal with the slaveJeids after Exception
     */
    private void dealSlaveException() {
        jedisSentinelSlavesPool.returnBrokenResource(slaveJedis);
        slaveJedis = null;

        jedisSentinelMasterPool.returnResource(masterJedis);
        masterJedis = null;
    }

    /**
     * the function to deal with the masterJeids after Exception
     */
    private void dealMasterException() {

        jedisSentinelMasterPool.returnBrokenResource(masterJedis);
        masterJedis = null;

        jedisSentinelSlavesPool.returnResource(slaveJedis);
        slaveJedis = null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if(masterJedis != null) {
            sb.append("master = " + masterJedis.getClient().getHost() + ":" + masterJedis.getClient().getPort());
            sb.append("\r");
        }
        if(slaveJedis != null) {
            sb.append("slave = " + slaveJedis.getClient().getHost() + ":" + slaveJedis.getClient().getPort());
        }
        return sb.toString();
    }

    private void checkMode(Mode mode) {
        if(mode == this.mode || this.mode == Mode.ReadWrite) {
            return;
        }
        throw new NedisException("Mode Error! " + mode);
    }

    public static enum Mode {
        ReadOnly,
        WriteOnly,
        ReadWrite;
    }
}
