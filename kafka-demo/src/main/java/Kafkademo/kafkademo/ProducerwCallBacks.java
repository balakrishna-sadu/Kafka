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

public class ProducerwCallBacks {
public static void main(String[] args) {
		
		String bootstrap_server = "127.0.0.1:9092";
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_server);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> producer =  new KafkaProducer<String,String>(props);
		
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("demo-kafka", "Program generated message="+Math.floor(Math.random()*10+1));
		
		producer.send(record, new Callback() {
			@Override
			public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
				
		        Logger logger=LoggerFactory.getLogger(ProducerwCallBacks.class);  
		        if (exception== null) {  
		            logger.info("\nSuccessfully received the details as: \n" +  
		                    "Topic : " + recordMetadata.topic() + "\n" +  
		                    "Partition : " + recordMetadata.partition() + "\n" +  
		                    "Offset : " + recordMetadata.offset() + "\n" +  
		                    "Timestamp : " + recordMetadata.timestamp());  
		        }  
		        else {  
		            logger.error("Can't produce,getting error",exception);  		  
		        }  
			} 		
		});  

		producer.flush();
		System.out.println("Message Published Successsfully");
		producer.close();
	}
}
