package name.zicat.relax.nedis.sentinel;

/**
 * store host and port infomation
 * @company Newegg Tech (China) Co, Ltd
 * @author lz31
 * @date 2014-2-25
 */

public class HostAndPort {
	
	String host;
	int port;

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof HostAndPort) {
			HostAndPort hp = (HostAndPort) obj;
			return port == hp.port && host.equals(hp.host);
		}
		return false;
	}

	@Override
	public String toString() {
		return host + ":" + port;
	}
	
}
