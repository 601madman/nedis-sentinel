package name.zicat.relax.nedis.test.biz;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import name.zicat.relax.nedis.biz.Nedis;
import org.junit.After;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;


public class MainTest {
	
	@Test
	public void expireTest() {
		
		Nedis nedis = null;
		try {
			
			nedis = new Nedis();
			String s1 = nedis.handle(new Nedis.ReadHandlerListener<String>() {
				
				@Override
				public String callback(Jedis jedis) {
					jedis.select(1);
					return jedis.randomKey();
				}
			});
			
			String s2 = nedis.handle(new Nedis.ReadPipelineHandlerListener<String>() {
				
				@Override
				public String callback(Pipeline pipeline) {
					pipeline.select(1);
					Response<String> res = pipeline.randomKey();
					pipeline.sync();
					return res.get();
				}
				
			});
			System.out.println(s1 + "\n" + s2);
		} finally {
			if(nedis != null) {
				nedis.returnResource();
			}
		}
	}
	
	@Test
	public void test() throws InterruptedException {
		Nedis nedis = new Nedis(Nedis.Mode.ReadOnly);
		nedis.randomKey(4);
		nedis.returnResource();
		
		nedis = new Nedis(Nedis.Mode.ReadOnly);
		nedis.randomKey(6);
		nedis.returnResource();
		
		nedis = new Nedis(Nedis.Mode.ReadWrite, "sentinel2.xml");
		nedis.randomKey(4);
		nedis.returnResource();
		
		nedis = new Nedis(Nedis.Mode.ReadWrite, "sentinel2.xml");
		nedis.randomKey(6);
		nedis.returnResource();
	}
	
	@After
	public void after() {
		Nedis.destroyPool();
	}
	
	@Test
	public void threadTest() throws InterruptedException, ExecutionException {
		int testcount = 10000;
		ExecutorService service = Executors.newFixedThreadPool(500);
		List<Future<?>> list = new ArrayList<Future<?>>(testcount);
		for(int i = 0; i < testcount; i++) {
			Future<?> future = service.submit(new Runnable() {
				@Override
				public void run() {
					try {
						test();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			});
			list.add(future);
		}
		for(Future<?>f : list) {
			f.get();
		}
		service.shutdown();
		
	}
}
