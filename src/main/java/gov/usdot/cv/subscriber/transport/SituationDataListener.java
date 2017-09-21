package gov.usdot.cv.subscriber.transport;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.MessageListener;

import org.apache.log4j.Logger;

import com.deleidos.rtws.core.net.jms.JMSBundler;
import com.oss.asn1.AbstractData;
import com.oss.asn1.Coder;

import gov.usdot.asn1.generated.j2735.semi.IntersectionSituationData;
import gov.usdot.asn1.generated.j2735.semi.VehSitDataMessage;
import gov.usdot.asn1.j2735.J2735Util;
import gov.usdot.cv.common.dialog.DataBundle;
import gov.usdot.cv.common.security.SecurityEnvelopeUtil;

public class SituationDataListener implements MessageListener {

	private static final Logger logger = Logger.getLogger(SituationDataListener.class);
	
	private static final String DUMBY_DEST_HOST = "127.0.0.1";
	private static final int DUMBY_DEST_PORT 	= 46751;
	private static final String DUMBY_CERTIFICATE = "some certificate text";
	
	private static final String VEH_SIT_DATA_INPUT_FORMAT = "vsdm";
	private static final String INTERSECTION_SIT_DATA_INPUT_FORMAT = "intersectionSitData";
	
	private String inputFormat;
	
	private JMSBundler bundler;
	
	private Coder coder;
	
	public static SituationDataListener newInstance() {
		return new SituationDataListener();
	}
	
	private SituationDataListener() {
		// Uses the newInstance() method to get an instance
		// of the listener class.
	}
	
	public void setInputFormat(String inputFormat) {
		if (inputFormat != null) {
			this.inputFormat = inputFormat.trim();
		}
	}
	
	public void setJMSBundler(JMSBundler bundler) {
		this.bundler = bundler;
	}
	
	public void setCoder(Coder coder) {
		this.coder = coder;
	}
	
	public void onMessage(Message msg) {
		logger.debug("Received a message from the JMS topic.");
		
		if (msg == null) {
			logger.warn("Received a null message, dropping ...");
			return;
		}
		
		if (! (msg instanceof BytesMessage)) {
			logger.warn("Received a non-bytes message, dropping ...");
			return;
		}
		
		try {
			BytesMessage bmsg = (BytesMessage) msg;
			byte [] packet = new byte[(int) bmsg.getBodyLength()];
			bmsg.readBytes(packet);
			
			DataBundle bundle = decrypt(packet);
			if (bundle == null) {
				logger.warn("Received a bytes message with a empty payload, dropping ...");
				return;
			}
			
			AbstractData pdu = J2735Util.decode(coder, bundle.getPayload());
			
			boolean valid = false;
			if (pdu != null && this.inputFormat != null) {
				logger.debug("Analyzing message type " + pdu.getClass().getName() + " ...");
				if ((inputFormat.equals(VEH_SIT_DATA_INPUT_FORMAT) && pdu instanceof VehSitDataMessage) || 
					(inputFormat.equals(INTERSECTION_SIT_DATA_INPUT_FORMAT) && pdu instanceof IntersectionSituationData)) {
					valid = true;
				}
			} 
			
			if (valid) {
				logger.debug("Encoding data bundle ...");
				String encoded = bundle.encode();
				logger.debug(String.format("Adding message '%s' to bundler for sending ...", encoded));
				bundler.appendln(encoded);
			} else {
				logger.debug(String.format("Received a data bundle that is not supported by the input data model '%s'.", this.inputFormat));
			}
		} catch(Exception ex) {
			logger.error("Failed to process and send bytes message.", ex);
		}
	}
	
	private DataBundle decrypt(byte [] payload) {
		if (payload == null || payload.length == 0) return null;
		payload = SecurityEnvelopeUtil.from1609_2(payload);
		DataBundle.Builder builder = new DataBundle.Builder();
		builder.setDestHost(DUMBY_DEST_HOST).setDestPort(DUMBY_DEST_PORT).setFromForwarder(false)
			.setCertificate(DUMBY_CERTIFICATE.getBytes()).setPayload(payload);
		return builder.build();
	}
	
}