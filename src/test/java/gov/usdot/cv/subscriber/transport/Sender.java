package gov.usdot.cv.subscriber.transport;

import javax.jms.BytesMessage;

import org.apache.commons.codec.binary.Base64;

import com.deleidos.rtws.commons.net.jms.BasicMessageProducer;
import com.deleidos.rtws.commons.net.jms.JMSFactory;
import com.deleidos.rtws.commons.net.jms.RoundRobinJMSConnectionFactory;

public class Sender {
	
	private static final String BASE64_SIT_DATA = "MHCAAgCKgQEBokWgKaATgAIH3oEBAoIBCoMBCIQBHoUBGYEBK4IBVYMCA0iEAQCFAgA3hwEkgQEAggECoxKDEP/u/rQAAABk//n+yAAAAGSDIAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
	
	private String brokerURL;
	
	public static Sender newInstance() {
		return new Sender();
	}
	
	private Sender() {
		// All new instances creation uses the newInstance() method
	}
	
	public void setBrokerURL(String brokerURL) {
		this.brokerURL = brokerURL;
	}
	
	public void send(String destination, int num, int delay) {
		RoundRobinJMSConnectionFactory cf = new RoundRobinJMSConnectionFactory();
		cf.setBrokerURL(this.brokerURL);

		JMSFactory factory = new JMSFactory();
		factory.setConnectionFactory(cf);
		BasicMessageProducer producer = 
				factory.createSimpleTopicProducer(destination);
		try {
			producer.open();
		
			byte [] data = Base64.decodeBase64(BASE64_SIT_DATA);
			for (int i = 0; i < num; i++) {
				try {
					BytesMessage bm = producer.createBytesMessage();
					bm.writeBytes(data);
					producer.send(bm);
					System.out.println("Message sent.");
				} catch (Exception ex) {
					System.out.println("Failed to send bytes message. Message: " + ex.getMessage());
				}
			
				try { Thread.sleep(delay); } catch (InterruptedException e) {}
			}
		} catch (Exception ex) {
			System.out.println("Failed to open connecton to remove jms server. Message: " + ex.getMessage());
		} finally {
			producer.close();
		}
	}
	
}