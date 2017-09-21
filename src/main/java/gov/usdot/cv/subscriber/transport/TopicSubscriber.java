package gov.usdot.cv.subscriber.transport;

import java.security.SecureRandom;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.log4j.Logger;

import com.deleidos.rtws.core.net.jms.JMSBundler;
import com.oss.asn1.Coder;

public class TopicSubscriber {

	private static final Logger logger = Logger.getLogger(TopicSubscriber.class);
	
	private String inputFormat;
	
	private String brokerUrl;
	private String topicName;
	
	private TrustManager [] trustManagers;
	private KeyManager [] keyManagers;
	private ActiveMQSslConnectionFactory cf;
	
	private Connection conn = null;
	private Session session = null;
	private MessageConsumer consumer = null;
	private JMSBundler bundler;
	private KeystoreLoader loader = null;
	private boolean terminate = false;
	private Coder coder = null;
	
	public static TopicSubscriber newInstance() {
		return new TopicSubscriber();
	}
	
	private TopicSubscriber() {
		// Uses the newInstance() method to get an instance
		// of the topic subscriber
	}
	
	public void setInputFormat(String inputFormat) {
		this.inputFormat = inputFormat;
	}
	
	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
	
	public void setBrokerURL(String brokerURL) {
		this.brokerUrl = brokerURL;
	}
	
	public void setDestinationBundler(JMSBundler bundler) {
		this.bundler = bundler;
	}
	
	public void setKeystoreLoader(KeystoreLoader loader) {
		this.loader = loader;
	}

	public void setCoder(Coder coder) {
		this.coder = coder;
	}
	
	public void stop() {
		logger.info("Stopping topic subscriber ...");
		this.terminate = true;
		if (this.conn != null) {
			try { this.conn.stop(); } catch (JMSException ignore) {}
		}
	}
	
	public void start() throws JMSException {
		logger.info("Initializing and starting topic subscriber ...");
		
		while (! terminate) try {
			buildConnectionFactory();
			
			this.conn = this.cf.createConnection();
			this.session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			this.consumer = session.createConsumer(session.createTopic(this.topicName));
			this.consumer.setMessageListener(buildSituationDataListener());
			
			break;
		} catch (Exception ex) {
			logger.error(String.format("Failed to initialize topic subscriber '%s'. Retrying in 5 seconds ...", this.topicName), ex);
			try { Thread.sleep(5000); } catch(Exception ignore) {}
		} 
		
		this.conn.start();
		
		logger.info("Initialized and started topic subscriber.");
	}
	
	private void buildConnectionFactory() throws Exception {
		if (this.cf != null) return;

		if (this.loader == null) {
			throw new NullPointerException("Keystore loader is null.");
		}
		
		if (this.trustManagers == null) {
			this.trustManagers = loader.getTrustManagers();
		}
		
		if (this.keyManagers == null) {
			this.keyManagers = loader.getKeyManagers();
		}
		
		this.cf = new ActiveMQSslConnectionFactory();
		this.cf.setBrokerURL(this.brokerUrl);
		this.cf.setKeyAndTrustManagers(keyManagers, trustManagers, new SecureRandom());
	}
	
	private SituationDataListener buildSituationDataListener() {
		SituationDataListener listener = SituationDataListener.newInstance();
		listener.setInputFormat(this.inputFormat);
		listener.setJMSBundler(this.bundler);
		listener.setCoder(this.coder);
		return listener;
	}
	
}