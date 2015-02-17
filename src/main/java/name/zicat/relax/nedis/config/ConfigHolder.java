package name.zicat.relax.nedis.config;

import name.zicat.relax.nedis.utils.NedisException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;


/**
 * xml parser
 * @company Newegg Tech (China) Co, Ltd
 * @author lz31
 * @date 2014-2-25
 */
public class ConfigHolder {
	
	public static final String DEFAULT_FILE_NAME = "sentinel.xml";
	
	public static NedisConfig getNedisConfigInstance() {
		return getNedisConfig(DEFAULT_FILE_NAME);
	}
	
	public static NedisConfig getNedisConfig(String fileName) {
		try {
			return initSentinelsConfig(fileName);
		} catch (JAXBException e) {
			throw new NedisException("parser config file " + fileName + " error! " +  e);
		}
	}
	
	private static NedisConfig initSentinelsConfig(String name) throws JAXBException {
		JAXBContext context = JAXBContext.newInstance(NedisConfig.class);
		Unmarshaller unmarshaller = context.createUnmarshaller();
		return (NedisConfig) unmarshaller.unmarshal(Thread.currentThread().getContextClassLoader().getResourceAsStream(name));
	}
}
