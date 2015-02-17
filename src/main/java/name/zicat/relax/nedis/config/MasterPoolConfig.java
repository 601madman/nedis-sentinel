package name.zicat.relax.nedis.config;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement( name="master-pool" )
public class MasterPoolConfig {
	private boolean lifo = true;
	private int maxActive = 24;
	private int maxIdle = 10;
	private int maxWait = 150000;
	private int minEvictableIdleTimeMillis = 100000;
	private int minIdle = 4;
	private boolean testOnBorrow = false;
	private boolean testOnReturn = false ;
	
	public boolean isLifo() {
		return lifo;
	}
	
	@XmlElement
	public void setLifo(boolean lifo) {
		this.lifo = lifo;
	}
	public int getMaxActive() {
		return maxActive;
	}
	
	@XmlElement
	public void setMaxActive(int maxActive) {
		this.maxActive = maxActive;
	}
	public int getMaxIdle() {
		return maxIdle;
	}
	
	@XmlElement
	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}
	public int getMaxWait() {
		return maxWait;
	}
	
	@XmlElement
	public void setMaxWait(int maxWait) {
		this.maxWait = maxWait;
	}
	public int getMinEvictableIdleTimeMillis() {
		return minEvictableIdleTimeMillis;
	}
	
	@XmlElement
	public void setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
		this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
	}
	public int getMinIdle() {
		return minIdle;
	}
	
	@XmlElement
	public void setMinIdle(int minIdle) {
		this.minIdle = minIdle;
	}
	public boolean isTestOnBorrow() {
		return testOnBorrow;
	}
	
	@XmlElement
	public void setTestOnBorrow(boolean testOnBorrow) {
		this.testOnBorrow = testOnBorrow;
	}
	public boolean isTestOnReturn() {
		return testOnReturn;
	}
	
	@XmlElement
	public void setTestOnReturn(boolean testOnReturn) {
		this.testOnReturn = testOnReturn;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("lifo:" + lifo);
		sb.append("\r");
		sb.append("maxActive:" + maxActive);
		sb.append("\r");
		sb.append("maxIdle:" + maxIdle);
		sb.append("\r");
		sb.append("maxWait:" + maxWait);
		sb.append("\r");
		sb.append("minEvictableIdleTimeMillis:" + minEvictableIdleTimeMillis);
		sb.append("\r");
		sb.append("minIdle:" + minIdle);
		sb.append("\r");
		sb.append("testOnBorrow :" + testOnBorrow );
		sb.append("\r");
		sb.append("testOnReturn :" + testOnReturn );
		return sb.toString();
	}
}
