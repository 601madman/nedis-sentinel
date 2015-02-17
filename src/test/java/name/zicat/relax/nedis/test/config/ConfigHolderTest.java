package name.zicat.relax.nedis.test.config;

import java.util.Set;

import name.zicat.relax.nedis.config.ConfigHolder;
import name.zicat.relax.nedis.config.NedisConfig;
import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * test xml parser
 * @company Newegg Tech (China) Co, Ltd
 * @author lz31
 * @date 2014-2-25
 */
public class ConfigHolderTest {
	
	private static final Logger logger = Logger.getLogger(ConfigHolderTest.class);
	
	@Test
	public void commonTest(){
		try{
			NedisConfig nedisConfig = ConfigHolder.getNedisConfigInstance();
			logger.warn(nedisConfig.toString());
		}catch(Exception e){
			logger.error(e.toString());
		}
	}
	
	@Test
	public void threadTest() throws InterruptedException {
		for(int i=0;i<20;i++){
			
			new Thread(new Runnable() {

				@Override
				public void run() {
					NedisConfig nedisConfig = ConfigHolder.getNedisConfigInstance();
					logger.warn(nedisConfig.toString());
				}
			}).start();
		}
		Thread.sleep(5*1000);
	}
	
	@Test
	public void testFunc(){
		try{
			NedisConfig nedisConfig = ConfigHolder.getNedisConfigInstance();
			Set<String>sentinelInfo = nedisConfig.getSentinelsConfig().getSentinelInfo();
			for(String str:sentinelInfo){
				logger.warn(str);	
			}
		}catch(Exception e){
			logger.error(e.toString());
		}
	}
}
