package gov.usdot.cv.subscriber.transport;

import java.io.File;

import javax.jms.JMSException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.deleidos.rtws.core.framework.Description;
import com.deleidos.rtws.core.framework.UserConfigured;
import com.deleidos.rtws.transport.AbstractTransportService;
import com.oss.asn1.Coder;
import com.oss.asn1.ControlTableNotFoundException;

import gov.usdot.asn1.generated.j2735.J2735;
import gov.usdot.cv.common.util.InstanceMetadataUtil;
import gov.usdot.cv.resources.PrivateResourceLoader;

@Description("Transports situation data from clearinghouse to warehouse via JMS subscription.")
public class JMSTopicListenerTransportService extends AbstractTransportService {

	private static final Logger logger = Logger.getLogger(JMSTopicListenerTransportService.class);

	private boolean terminate = false;
	
	private String topicName;
	private String protocol;
	private String targetHost;
	private int targetPort;
	private String keystorePath;
	private String truststorePath;
	
	private TopicSubscriber subscriber;

	private Coder coder = null;
	
	@UserConfigured(value="cv.sitdata", description="The id used to subscribe for situation data.")
	public void setSdcTopicName(String topicName) {
		if (topicName != null) {
			this.topicName = topicName.trim();
		}
	}
	
	public String getSdcTopicName() {
		return this.topicName;
	}
	
	@UserConfigured(value="ssl", description="The hostname or ip address of the JMS server.")
	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}
	
	public String getProtocol() {
		return this.protocol;
	}
	
	@UserConfigured(value="", description="The hostname or ip address of the JMS server.")
	public void setTargetHost(String targetHost) {
	      this.targetHost = targetHost;
	}
	
	public String getTargetHost() {
		return this.targetHost;
	}
	
	@UserConfigured(value="61616", description="The port of the JMS server.")
	public void setTargetPort(int targetPort) {
		this.targetPort = targetPort;
	}
	
	public int getTargetPort() {
		return this.targetPort;
	}
	
	public String getKeystorePath() {
		return this.keystorePath;
	}
	
	@UserConfigured(
		value = "s3://cv-sdw/dev/keystore/ssl-keystore", 
		description = "Absolute path of the pubsub server keystore file.")
	public void setKeystorePath(String keystorePath) {
		if (keystorePath != null) this.keystorePath = keystorePath.trim();
	}
	
	public String getTruststorePath() {
		return this.truststorePath;
	}
	
	@UserConfigured(
			value = "s3://cv-sdw/dev/keystore/ssl-truststore", 
			description = "Absolute path of the pubsub server truststore file.")
	public void setTruststorePath(String truststorePath) {
		if (truststorePath != null) this.truststorePath = truststorePath.trim();
	}

	public void execute() {
		logger.info("Starting JMS topic listener transport service ...");

		try {
			J2735.initialize();
			coder = J2735.getPERUnalignedCoder();
		} catch (ControlTableNotFoundException ex) {
			log.error("Couldn't initialize J2735 parser", ex);
		} catch (com.oss.asn1.InitializationException ex) {
			log.error("Couldn't initialize J2735 parser", ex);
		}
		
		if (InstanceMetadataUtil.getNodeNumber() != 1) {
			logger.info("Not first transport node, exiting JMS topic listener transport service ...");
			return;
		}
		
		if (this.topicName == null || this.topicName.length() == 0) {
			logger.info("Topic name not configured, exiting JMS topic listener transport service ...");
			return;
		}
		
		StringBuilder brokerURL = new StringBuilder();
		brokerURL.append(this.protocol).append("://").append(this.targetHost).append(':').append(this.targetPort);
		
		this.subscriber = TopicSubscriber.newInstance();
		this.subscriber.setInputFormat(this.getInputFormat());
		this.subscriber.setTopicName(this.topicName);
		this.subscriber.setBrokerURL(brokerURL.toString());
		this.subscriber.setDestinationBundler(this.bundler);
		this.subscriber.setKeystoreLoader(getKeystoreLoader());
		this.subscriber.setCoder(coder);
		
		try {
			this.subscriber.start();
		} catch (JMSException jmse) {
			logger.error("Failed to start JMS topic subscriber.", jmse);
			this.terminate = true;
		}
		
		// Keep the transport service thread running until 
		// notified to terminate.
		
		while (! terminate) {
			try { Thread.sleep(5000); } catch (InterruptedException ie) {}
		}
		
		this.subscriber.stop();
		
		logger.info("Exiting JMS topic listener transport service ...");
	}

	public void terminate() {
		this.terminate = true;
		if (this.subscriber != null) {
			this.subscriber.stop();
		}

		coder = null;
		J2735.deinitialize();
	}
	
	private KeystoreLoader getKeystoreLoader() {
		String storePwd = PropertyLocator.getString("external.topic.store.password",
										PrivateResourceLoader.getProperty(
											"@transport-listener/jms.topic.listener.transport.service.password@"));
		
		KeystoreLoader loader = null;
		
		// If the keystore or truststore configuration is missing we'll uses the default file system
		// keystore loader and looking in the properties file for the keystore and truststore path.
		
		if ((StringUtils.isBlank(this.keystorePath) && StringUtils.isBlank(this.truststorePath)) ||
			(StringUtils.isBlank(this.keystorePath) && ! StringUtils.isBlank(this.truststorePath)) ||
			(! StringUtils.isBlank(this.keystorePath) && StringUtils.isBlank(this.truststorePath))) {
			logger.info("Keystore and/or truststore path not configured, setting listener to uses file system keystore loader.");
			loader = new FileSystemKeystoreLoader.Builder().build();
		}
		
		if (! StringUtils.isBlank(this.keystorePath) && ! StringUtils.isBlank(this.truststorePath)) {
			if (this.keystorePath.startsWith("s3://") && this.truststorePath.startsWith("s3://")) {
				logger.info("Setting listener to uses s3 keystore loader.");
				S3KeystoreLoader.Builder builder = new S3KeystoreLoader.Builder();
				builder.setKeystorePath(keystorePath).setTruststorePath(truststorePath).setStorePwd(storePwd);
				loader = builder.build();
			} else if (this.keystorePath.startsWith(File.separator) && this.truststorePath.startsWith(File.separator)) {
				logger.info("Setting listener to uses file system keystore loader.");
				FileSystemKeystoreLoader.Builder builder = new FileSystemKeystoreLoader.Builder();
				builder.setKeystorePath(this.keystorePath).setTruststorePath(this.truststorePath).setStorePwd(storePwd);
				loader = builder.build();
			} else {
				logger.info("Invalid keystore [" + this.keystorePath + "] and truststore [" +  
					this.truststorePath + "] path configuration, no keystore loader set.");
			}
		}
		
		return loader;
	}
	
	//
	// Hide the following parameters because they are not needed
	//

	@Override
	@UserConfigured(
		value = "BER_ASN-1",
		flexValidator = { "RegExpValidator expression=" + "^(BER_ASN-1)$" },
		description = "BER_ASN-1 (read only)", 
		convertToSystemConfigured="true")	
	public void setRecordFormat(String recordFormat) {}

	@Override
	@UserConfigured(
		value = "0",
		flexValidator = { "RegExpValidator expression=" + "^([0])$" },
		description = "Not applicable (read only)", 
		convertToSystemConfigured="true")			
	public void setRecordHeaderLines(int recordHeaderLines) {}

}