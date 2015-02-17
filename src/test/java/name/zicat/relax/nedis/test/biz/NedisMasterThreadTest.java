package name.zicat.relax.nedis.test.biz;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import name.zicat.relax.nedis.biz.Nedis;
import name.zicat.relax.nedis.utils.NedisException;
import org.junit.Test;


public class NedisMasterThreadTest {
	
	ExecutorService exe = Executors.newFixedThreadPool(20);
	
	@Test
	public void threadTest() throws InterruptedException {
		for(int j=0;j<40000;j++){
			for(int i=0; i<20; i++) {
				final int k = j*20 + i;
				exe.execute(new Runnable() {
					@Override
					public void run() {
						userNedis(k);
					}
				});
			}
			Thread.sleep(2*1000);
		}
		Thread.sleep(30*1000);
		Nedis.destroyPool();
		System.out.println("finished");
	}
	
	
	
	public static void userNedis(int k) {
		Nedis nedis = null;
		try {
			nedis = new Nedis();
			String keyValue = String.valueOf(k);
			System.out.println(nedis.addString(9, keyValue, keyValue));
		} catch (Exception e) {
			throw new NedisException(e);
		} finally {
			if(nedis != null) {
				nedis.returnResource();
			}
		}
	}
}
