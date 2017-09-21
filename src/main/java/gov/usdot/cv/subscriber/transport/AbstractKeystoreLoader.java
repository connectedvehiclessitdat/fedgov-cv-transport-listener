package gov.usdot.cv.subscriber.transport;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Enumeration;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public abstract class AbstractKeystoreLoader implements KeystoreLoader {
	protected KeyManager [] getKeyManagers(
			String type,
			File keyStoreFile,
			String keyStorePassword, 
			String certAlias) throws Exception {
		KeyStore keyStore = KeyStore.getInstance(type);
		char[] keyStorePwd = (keyStorePassword != null) ? keyStorePassword.toCharArray() : null;
		keyStore.load(new FileInputStream(keyStoreFile), keyStorePwd);

		// if cert alias given then load single cert with given alias
		if (certAlias != null) {
			Enumeration<String> aliases = keyStore.aliases();
			while (aliases.hasMoreElements()) {
				String alias = aliases.nextElement();

				if (!certAlias.equals(alias)) {
					// remove cert only load cert with given alias
					keyStore.deleteEntry(alias);
				}
			}
		}
		
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(keyStore, keyStorePwd);
		return kmf.getKeyManagers();
	}
	
	protected TrustManager [] getTrustManagers(
			String type, 
			File trustStoreFile,
			String trustStorePassword) throws Exception {
		KeyStore trustStore = KeyStore.getInstance(type);
		char [] trustStorePwd = (trustStorePassword != null) ? trustStorePassword.toCharArray() : null;
		trustStore.load(new FileInputStream(trustStoreFile), trustStorePwd);
		
		TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustManagerFactory.init(trustStore);
		return trustManagerFactory.getTrustManagers();
	}
}