package name.zicat.relax.nedis.utils;

/**
 * 
 * @company Newegg Tech (China) Co, Ltd
 * @author lz31
 * @date 2014-2-25
 */
public class NedisException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5191751053193161838L;
	
	public NedisException() {
		super();
	}

	public NedisException(String message, Throwable cause) {
		super(message, cause);
	}

	public NedisException(String message) {
		super(message);
	}

	public NedisException(Throwable cause) {
		super(cause);
	}
	
}
