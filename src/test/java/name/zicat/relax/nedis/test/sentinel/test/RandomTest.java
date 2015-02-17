package name.zicat.relax.nedis.test.sentinel.test;

import java.util.Random;

public class RandomTest {
	
	public static final Random random = new Random();
	
	public static void main(String[] args) {
		threadTest();
	}
	
	public static void print() {
		for(int i=0; i<10; i++) {
			System.out.println(random.nextInt(10));
		}
	}
	
	public static void threadTest() {
		for(int i=0; i<10; i++) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					print();
				}
			}).start();
		}
	}
}
