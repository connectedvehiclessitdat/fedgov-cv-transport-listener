package gov.usdot.cv.subscriber.transport;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

public interface KeystoreLoader {
	public String JAVA_KEYSTORE_TYPE = "JKS";
	
	public KeyManager [] getKeyManagers() throws Exception;
	public TrustManager [] getTrustManagers() throws Exception;
}