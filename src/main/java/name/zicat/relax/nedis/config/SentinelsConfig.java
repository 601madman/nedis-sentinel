package name.zicat.relax.nedis.config;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import redis.clients.jedis.Protocol;

/**
 * 
 * @company Newegg Tech (China) Co, Ltd
 * @author lz31
 * @date 2014-2-25
 */
@XmlRootElement( name="sentinels")
public class SentinelsConfig {
	
	private List<SentinelConfig> sentinelsConfig;
	
	private String masterName;
	
	private int reflashWaitTimeMillis = 1000;
	
	private int soTimeout = Protocol.DEFAULT_TIMEOUT;
	
	private boolean slaveBackUp = false;
	
	public List<SentinelConfig> getSentinelsConfig() {
		return sentinelsConfig;
	}

	@XmlElement( name="sentinel" )
	public void setSentinelsConfig(List<SentinelConfig> sentinelsConfig) {
		this.sentinelsConfig = sentinelsConfig;
	}
	
	
	public String getMasterName() {
		return masterName;
	}
	
	@XmlAttribute
	public void setMasterName(String masterName) {
		this.masterName = masterName;
	}

	public int getReflashWaitTimeMillis() {
		return reflashWaitTimeMillis;
	}

	@XmlAttribute
	public void setReflashWaitTimeMillis(int reflashWaitTimeMillis) {
		this.reflashWaitTimeMillis = reflashWaitTimeMillis;
	}
		
	
	public int getSoTimeout() {
		return soTimeout;
	}
	
	@XmlAttribute
	public void setSoTimeout(int soTimeout) {
		this.soTimeout = soTimeout;
	}
	
	public boolean isSlaveBackUp() {
		return slaveBackUp;
	}
	
	@XmlAttribute
	public void setSlaveBackUp(boolean slaveBackUp) {
		this.slaveBackUp = slaveBackUp;
	}

	public Set<String> getSentinelInfo() {
		Set<String>sentinelInfo = new HashSet<String>();
		if(sentinelsConfig != null && !sentinelsConfig.isEmpty()) {
			for(SentinelConfig sentinelConfig : sentinelsConfig) {
				String hostAndPort = sentinelConfig.getHost() + ":" + sentinelConfig.getPort();
				sentinelInfo.add(hostAndPort);
			}
		}
		return sentinelInfo;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(SentinelConfig sentinelConfig : sentinelsConfig) {
			sb.append(sentinelConfig.toString());
			sb.append("\r");
		}
		return sb.toString();
	}
}
