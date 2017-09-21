package gov.usdot.cv.subscriber.transport;

import static org.junit.Assert.assertTrue;

import java.util.List;

import javax.jms.Message;

import org.apache.activemq.broker.BrokerService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.deleidos.rtws.commons.config.RtwsConfig;

public class SituationDataListenerTest {
	
	private static final String REMOTE_JMS_BROKER_NAME = "lcsdw-pubsub";
	private static final String REMOTE_JMS_CONNECTOR_URL = "tcp://localhost:61619";
	
	private static final String JMS_EXT_BROKER_NAME = "rhsdw-jms-ext";
	private static final String JMS_EXT_CONNECTOR_URL = "tcp://localhost:61620";
	
	private static BrokerService remoteBroker;
	private static BrokerService rhsdwBroker;
	
	private Sender sender = Sender.newInstance();
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		System.out.println("Starting remote jms broker service ...");
		remoteBroker = new BrokerService();
		remoteBroker.setBrokerName(REMOTE_JMS_BROKER_NAME);
		remoteBroker.addConnector(REMOTE_JMS_CONNECTOR_URL);
		remoteBroker.setPersistent(false);
		remoteBroker.setUseJmx(false);
		remoteBroker.start();
		

		System.out.println("Starting jms-ext broker service ...");
		rhsdwBroker = new BrokerService();
		rhsdwBroker.setBrokerName(JMS_EXT_BROKER_NAME);
		rhsdwBroker.addConnector(JMS_EXT_CONNECTOR_URL);
		rhsdwBroker.setPersistent(false);
		rhsdwBroker.setUseJmx(false);
		rhsdwBroker.start();
	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		System.out.println("Stopping remote jms broker service ...");
		remoteBroker.stop();
		System.out.println("Stopping jms-ext broker service ...");
		rhsdwBroker.stop();
	}
	
	@Test @Ignore
	public void testReceivingMessageFromTopic() {
		System.out.println(">>> Running testReceivingMessageFromTopic() ...");
		
		String subscriberId = "10052589";
		String queue = "com.rtsaic.parse";
		
		RtwsConfig.getInstance().getConfiguration().setProperty("messaging.header.format", "Data.Format");		
		
		JMSTopicListenerTransportService svc = new JMSTopicListenerTransportService();
		svc.setProtocol("tcp");
		svc.setTargetHost("localhost");
		svc.setTargetPort(61619);
		svc.setSdcTopicName(subscriberId);
		
		svc.setUrl(JMS_EXT_CONNECTOR_URL);
		svc.setUser(RtwsConfig.getInstance().getString("messaging.external.connection.user"));
		svc.setPassword(RtwsConfig.getInstance().getString("messaging.external.connection.password"));
		svc.setPersistent(false);
		svc.setInputFormat("vsdmsitdata");
		svc.setQueue(queue);
		svc.initialize();
		
		TransportService ts = new TransportService();
		ts.setTransportService(svc);
		
		Receiver r = Receiver.newInstance();
		r.setBrokerURL(JMS_EXT_CONNECTOR_URL);
		r.setQueue(queue);
		
		Thread r_receiver = new Thread(r);
		Thread t_transport = new Thread(ts);
		
		r_receiver.start();
		t_transport.start();
		
		try {
			// Give the thread some time to initialize and setup the listener
			Thread.sleep(3000);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		
		sender.setBrokerURL(REMOTE_JMS_CONNECTOR_URL);
		sender.send(subscriberId, 5, 100);
		
		try {
			// Give the thread some time to initialize and setup the listener
			Thread.sleep(5000);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		
		r.terminate();
		svc.terminate();
		
		try { r_receiver.join(); t_transport.join(); } catch (InterruptedException ignore) {}

		List<Message> messages = r.getMessages();
		assertTrue("Expecting 5 messages on the parse queue.", messages.size() == 5);
	}
	
	private class TransportService implements Runnable {
		private JMSTopicListenerTransportService service;
		
		public void setTransportService(JMSTopicListenerTransportService service) {
			this.service = service;
		}
		
		public void run() {
			service.execute();
		}
	}
	
}