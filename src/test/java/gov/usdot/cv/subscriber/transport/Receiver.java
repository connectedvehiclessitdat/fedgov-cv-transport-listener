package gov.usdot.cv.subscriber.transport;

import java.util.ArrayList;
import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import com.deleidos.rtws.commons.net.jms.RoundRobinJMSConnectionFactory;

public class Receiver implements Runnable {
	private String brokerURL;
	private String queue;
	private boolean terminate = false;
	private List<Message> received = new ArrayList<Message>();

	public static Receiver newInstance() {
		return new Receiver();
	}
	
	private Receiver() {
		// All new instances creation uses the newInstance() method
	}
	
	public void setBrokerURL(String brokerURL) {
		this.brokerURL = brokerURL;
	}
	
	public void setQueue(String queue) {
		this.queue = queue;
	}
	
	public void terminate() {
		this.terminate = true;
	}
	
	public List<Message> getMessages() {
		return this.received;
	}
	
	public void run() {
		consume(this.queue);
	}

	private void consume(String queue) {
		RoundRobinJMSConnectionFactory cf = new RoundRobinJMSConnectionFactory();
		cf.setBrokerURL(this.brokerURL);
		
		Connection connection = null;
		Session session = null;
		MessageConsumer consumer = null;
		
		try {
			connection = cf.createConnection();
			session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			consumer = session.createConsumer(session.createQueue(queue));
			
			while (! terminate) {
				try {
					Message message = consumer.receive(100);
					if (message == null) continue;
					if (message instanceof BytesMessage) {
						System.out.println("Message received.");
						this.received.add(message);
					}
					message.acknowledge();
				} catch (Exception ex) {
					System.out.println("Consumer receive failed. Message: " + ex.getMessage());
				} 
			}
		} catch (Exception ex) {
			System.out.println("Failed to open connecton to remove jms server. Message: " + ex.getMessage());
		} finally {
			try {consumer.close();} catch (Exception ignore) { }
	        try {session.close();} catch (Exception ignore) { }
	        try {connection.close();} catch (Exception ignore) { }
		}
	}
}