package name.zicat.relax.nedis.test.biz;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import name.zicat.relax.nedis.biz.Nedis;
import name.zicat.relax.nedis.utils.NedisException;
import org.junit.Test;


/**
 * test thread safe
 * @company Newegg Tech (China) Co, Ltd
 * @author lz31
 * @date 2014-2-26
 */
public class NedisThreadTest {
	
	ExecutorService exe = Executors.newFixedThreadPool(25);
	
	@Test
	public void threadTest() throws InterruptedException {
		for(int j=0;j<6000000;j++){
			for(int i=0; i<20; i++) {
				final int k = i;
				exe.execute(new Runnable() {
					@Override
					public void run() {
						userNedis(k);
					}
				});
			}
		}
		Thread.sleep(60*1000);
		Nedis.destroyPool();
		exe.shutdown();
		System.out.println("finished");
	}
	
	
	
	public static void userNedis(int k) {
		Nedis nedis = null;
		try {
			nedis = new Nedis();
		} catch (Exception e) {
			throw new NedisException(e);
		} finally {
			if(nedis != null) {
				nedis.returnResource();
			}
		}
	}
}
