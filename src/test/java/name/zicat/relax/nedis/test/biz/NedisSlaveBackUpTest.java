/**
 * lz31
 * 2014-7-16
 */
package name.zicat.relax.nedis.test.biz;

import name.zicat.relax.nedis.biz.Nedis;
import org.junit.Test;


/**
 * @author lz31
 * 2014-7-16
 */
public class NedisSlaveBackUpTest {
	
	@Test
	public void test() {
		Nedis nedis = new Nedis(Nedis.Mode.ReadOnly);
		nedis.randomKey(1);
		nedis.returnResource();
		Nedis.destroyPool();
	}
}
