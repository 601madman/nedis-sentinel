package name.zicat.relax.nedis.test.sentinel.test;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

public class AtomicBooleanTest {
	public final static AtomicBoolean  TEST_BOOLEAN = new AtomicBoolean(false);
	
	@Test
	public void test() throws InterruptedException {
		for(int i=0; i<10;i++){
			final int j = i;
			new Thread(){
				public void run(){
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if(TEST_BOOLEAN.compareAndSet(false, true)){
						System.out.println(j + "succeed!");					
					}
				}
			}.start();
		}
		Thread.sleep(5*1000);
	}
}
