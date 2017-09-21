package gov.usdot.cv.subscriber.transport;

import com.deleidos.rtws.commons.config.RtwsConfig;

public class PropertyLocator {
	
	private PropertyLocator() {
		// All invocation through static methods.
	}
	
	public static String getString(String key, String defaultValue) {
		String value = getString(key);
		if (value == null) {
			value = defaultValue;
		}
		return value;
	}
	
	public static String getString(String key) {
		String value = System.getProperty(key);
		if (value == null) {
			value = RtwsConfig.getInstance().getString(key);
		}
		return value;
	}
	
}