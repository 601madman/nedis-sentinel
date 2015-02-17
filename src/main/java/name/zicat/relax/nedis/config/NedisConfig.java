package name.zicat.relax.nedis.config;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

@XmlRootElement( name = "nedis")
public class NedisConfig {
	
	private SentinelsConfig sentinelsConfig;
	private SlavePoolConfig slavePoolConfig;
	private MasterPoolConfig masterPoolConfig;
	
	public SentinelsConfig getSentinelsConfig() {
		return sentinelsConfig;
	}
	
	@XmlElement(name = "sentinels")
	public void setSentinelsConfig(SentinelsConfig sentinelsConfig) {
		this.sentinelsConfig = sentinelsConfig;
	}
	
	public SlavePoolConfig getSlavePoolConfig() {
		return slavePoolConfig;
	}
	
	@XmlElement(name="slave-pool")
	public void setSlavePoolConfig(SlavePoolConfig slavePoolConfig) {
		this.slavePoolConfig = slavePoolConfig;
	}
	
	public MasterPoolConfig getMasterPoolConfig() {
		return masterPoolConfig;
	}
	
	@XmlElement(name="master-pool")
	public void setMasterPoolConfig(MasterPoolConfig masterPoolConfig) {
		this.masterPoolConfig = masterPoolConfig;
	}

	public GenericObjectPoolConfig getSlaveConfig() {
		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		config.setLifo(slavePoolConfig.isLifo());
		config.setMaxIdle(slavePoolConfig.getMaxIdle());
		config.setMaxTotal(slavePoolConfig.getMaxActive());
		config.setMaxWaitMillis(slavePoolConfig.getMaxWait());
		config.setMinEvictableIdleTimeMillis(slavePoolConfig.getMinEvictableIdleTimeMillis());
		config.setMinIdle(slavePoolConfig.getMinIdle());
		config.setTestOnBorrow(slavePoolConfig.isTestOnBorrow());
		config.setTestOnReturn(slavePoolConfig.isTestOnReturn());
		return config;
	}

	public GenericObjectPoolConfig getMasterConfig() {
		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		config.setLifo(masterPoolConfig.isLifo());
		config.setMaxIdle(masterPoolConfig.getMaxIdle());
		config.setMaxTotal(masterPoolConfig.getMaxActive());
		config.setMaxWaitMillis(masterPoolConfig.getMaxWait());
		config.setMinEvictableIdleTimeMillis(masterPoolConfig.getMinEvictableIdleTimeMillis());
		config.setMinIdle(masterPoolConfig.getMinIdle());
		config.setTestOnBorrow(masterPoolConfig.isTestOnBorrow());
		config.setTestOnReturn(masterPoolConfig.isTestOnReturn());
		return config;
	}
}
