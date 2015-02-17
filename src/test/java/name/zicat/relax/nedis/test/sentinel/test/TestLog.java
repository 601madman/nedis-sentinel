package name.zicat.relax.nedis.test.sentinel.test;

import org.apache.log4j.Logger;
import org.junit.Test;

public class TestLog {
	private static final Logger LOG = Logger.getLogger(TestLog.class);
	
	@Test
	public void test(){
		LOG.info("test info");
		LOG.warn("test warn");
		LOG.error("test error");
	}
}
