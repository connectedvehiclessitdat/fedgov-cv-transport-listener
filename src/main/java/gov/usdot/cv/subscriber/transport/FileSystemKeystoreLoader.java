package gov.usdot.cv.subscriber.transport;

import java.io.File;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

import gov.usdot.cv.resources.PrivateResourceLoader;

public class FileSystemKeystoreLoader extends AbstractKeystoreLoader {
	private String keystorePath;
	private String truststorePath;
	private String storePwd;
	
	private FileSystemKeystoreLoader(
		String keystorePath,
		String truststorePath,
		String storePwd) {
		this.keystorePath = keystorePath;
		this.truststorePath = truststorePath;
		this.storePwd = storePwd;
	}
	
	public KeyManager [] getKeyManagers() throws Exception {
		return getKeyManagers(
				JAVA_KEYSTORE_TYPE, 
				new File(keystorePath), 
				storePwd, "");
	}
	
	public TrustManager [] getTrustManagers() throws Exception {
		return getTrustManagers(
			JAVA_KEYSTORE_TYPE, 
			new File(this.truststorePath), 
			this.storePwd);
	}
	
	public static class Builder {
		private String keystorePath;
		private String truststorePath;
		private String storePwd;
		
		public Builder setKeystorePath(String keystorePath) {
			this.keystorePath = keystorePath;
			return this;
		}
		
		public Builder setTruststorePath(String truststorePath) {
			this.truststorePath = truststorePath;
			return this;
		}
		
		public Builder setStorePwd(String storePwd) {
			this.storePwd = storePwd;
			return this;
		}
		
		public KeystoreLoader build() {
			// You either set them all or we get it from the properties file
			if (this.keystorePath == null || this.truststorePath == null || this.storePwd == null) {
				this.keystorePath = PropertyLocator.getString("external.topic.keystore", "/mnt/rdafs/lcsdw/keystore/ssl-keystore");
				this.truststorePath = PropertyLocator.getString("external.topic.truststore", "/mnt/rdafs/lcsdw/keystore/ssl-truststore");
				this.storePwd = PropertyLocator.getString("external.topic.store.password", 
									PrivateResourceLoader.getProperty("@transport-listener/file.system.keystore.loader.password@"));
			}
			
			return new FileSystemKeystoreLoader(
				this.keystorePath, 
				this.truststorePath,
				this.storePwd);
		}
	}
	
}