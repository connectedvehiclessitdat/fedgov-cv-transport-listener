package gov.usdot.cv.subscriber.transport;

import java.io.File;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

import com.deleidos.rtws.commons.cloud.exception.StorageException;
import com.deleidos.rtws.commons.cloud.platform.StorageInterface;
import com.deleidos.rtws.commons.cloud.util.InterfaceConfig;

public class S3KeystoreLoader extends AbstractKeystoreLoader {
	private static final String FS_KEYSTORE_DIR_PATH 	= "/tmp/rhsdw/keystore";
	private static final String KEYSTORE_FILENAME 		= "ssl-keystore";
	private static final String TRUSTSTORE_FILENAME 	= "ssl-truststore";
	
	private static final String KEYSTORE_FILEPATH 	= FS_KEYSTORE_DIR_PATH + File.separator + KEYSTORE_FILENAME;
	private static final String TRUSTSTORE_FILEPATH = FS_KEYSTORE_DIR_PATH + File.separator + TRUSTSTORE_FILENAME;
	
	private String keystorePath;
	private String truststorePath;
	private String storePwd;
	
	private S3KeystoreLoader(
		String keystorePath,
		String truststorePath,
		String storePwd) {
		this.keystorePath = keystorePath;
		this.truststorePath = truststorePath;
		this.storePwd = storePwd;
	}
	
	public KeyManager [] getKeyManagers() throws Exception {
		downloadStores();
		return getKeyManagers(
				JAVA_KEYSTORE_TYPE, 
				new File(KEYSTORE_FILEPATH), 
				storePwd, "");
	}
	
	public TrustManager [] getTrustManagers() throws Exception {
		downloadStores();
		return getTrustManagers(
			JAVA_KEYSTORE_TYPE, 
			new File(TRUSTSTORE_FILEPATH), 
			this.storePwd);
	}
	
	private void downloadStores() throws StorageException {
		File dir = new File(FS_KEYSTORE_DIR_PATH);
		if (! dir.exists()) {
			dir.mkdirs();
		}
		
		StorageInterface service = InterfaceConfig.getInstance().getStorageInterface();
		
		File keystore = new File(FS_KEYSTORE_DIR_PATH + File.separator + KEYSTORE_FILENAME);
		if (! keystore.exists()) {
			String keystoreWithoutScheme = this.keystorePath.replaceFirst("s3:\\/\\/", "");
			int idx = keystoreWithoutScheme.indexOf("/");
			String bucketName = keystoreWithoutScheme.substring(0, idx);
			String fileKey = keystoreWithoutScheme.substring(idx + 1);
			
			service.getFile(bucketName, fileKey, keystore);
		}
		
		File truststore = new File(FS_KEYSTORE_DIR_PATH + File.separator + TRUSTSTORE_FILENAME);
		if (! truststore.exists()) {
			String truststoreWithoutScheme = this.truststorePath.replaceFirst("s3:\\/\\/", "");
			int idx = truststoreWithoutScheme.indexOf("/");
			String bucketName = truststoreWithoutScheme.substring(0, idx);
			String fileKey = truststoreWithoutScheme.substring(idx + 1);
			
			service.getFile(bucketName, fileKey, truststore);
		}
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
			if (this.keystorePath == null) {
				throw new NullPointerException("Keystore path is null.");
			}
			
			if (this.truststorePath == null) {
				throw new NullPointerException("Truststore path is null.");
			}
			
			if (this.storePwd == null) {
				throw new NullPointerException("Store password is null.");
			}
			
			return new S3KeystoreLoader(
				this.keystorePath, 
				this.truststorePath,
				this.storePwd);
		}
	}
	
}