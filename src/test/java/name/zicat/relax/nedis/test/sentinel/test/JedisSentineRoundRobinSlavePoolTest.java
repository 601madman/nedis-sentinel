package name.zicat.relax.nedis.test.sentinel.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import name.zicat.relax.nedis.config.ConfigHolder;
import name.zicat.relax.nedis.config.SentinelsConfig;
import name.zicat.relax.nedis.sentinel.JedisSentinelSlavePool;
import redis.clients.jedis.Jedis;

/***
 * 对JedisSentineRoundRobinSlavePoolTest
 * @company Newegg Tech (China) Co, Ltd
 * @author lz31
 * @date 2014-2-25
 */
public class JedisSentineRoundRobinSlavePoolTest {
	
	@org.junit.Test
	public void test() throws InterruptedException {
		SentinelsConfig sentinelsConfig = ConfigHolder.getNedisConfigInstance().getSentinelsConfig();
		final JedisSentinelSlavePool pool = new JedisSentinelSlavePool("master", sentinelsConfig.getSentinelInfo(), sentinelsConfig.getReflashWaitTimeMillis());
		ExecutorService executor = Executors.newCachedThreadPool();
		for(int j=0;j<1;j++){
			for(int i=0;i<20;i++){
				executor.execute(new Runnable() {
					public void run() {
						Jedis jedis = pool.getResource();
						if(jedis != null){
							try{
								jedis.select(1);
								System.out.println(jedis.hashCode()+"|"+jedis.getClient().getHost()+":"+jedis.getClient().getPort()+"|"+jedis.randomKey());							
								pool.returnResource(jedis);														
							}catch(Exception e){
								System.out.println(e);
								pool.returnBrokenResource(jedis);
							}
						}
					}
				});
			}
			Thread.sleep(10000);
		}
		executor.shutdown();
		Thread.sleep(10000);
		pool.destroy();
	}
}
