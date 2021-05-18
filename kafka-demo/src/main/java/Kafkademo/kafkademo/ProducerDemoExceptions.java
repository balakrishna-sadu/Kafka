package Kafkademo.kafkademo;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoExceptions {
	static KafkaProducer<String, String> producer;
	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ProducerDemoExceptions.class);
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		producer = new KafkaProducer<String, String>(props);
		String topic = "hello-world";

		for(int i = 0;i<1;i++) {
			String value = "hello world "+Integer.toString(i);
			String key = "id_"+Integer.toString(i);

			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
			try {
				producer.send( record, new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {

						if( exception == null) {
							logger.info("\nSuccessfully received the details as: \n" +  
									"Topic : " + metadata.topic() + "\n" +  
									"Partition : " + metadata.partition() + "\n" +  
									"Offset : " + metadata.offset() + "\n" +  
									"Timestamp : " + metadata.timestamp());
						}
						else {
							logger.error("****************\nError Occured\n"+exception);
						}
					}
				}).get();
			}
			catch(Exception e) {
				System.out.println("*****************************\nERRRRRRRRRRRRROR\n");
			}
		}
		producer.flush();
		System.out.println("Message Published Successsfully");
		producer.close();
	}

}
