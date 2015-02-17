package name.zicat.relax.nedis.config;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @company Newegg Tech (China) Co, Ltd
 * @author lz31
 * @date 2014-2-25
 */
@XmlRootElement( name="sentinel" )
public class SentinelConfig {
	
	private String host;
	private int port;
	
	public String getHost() {
		return host;
	}
	
	@XmlAttribute
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	
	@XmlAttribute
	public void setPort(int port) {
		this.port = port;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(host + ":" + port);
		return sb.toString();
	}
}
