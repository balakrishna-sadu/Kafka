package Kafkademo.kafkademo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
	public static void main(String[] args) {
		
		String bootstrap_server = "127.0.0.1:9092";
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_server);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> producer =  new KafkaProducer<String,String>(props);
		
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("demo-kafka", "Program generated message="+Math.floor(Math.random()*10+1));
		
		producer.send(record);
		producer.flush();
		
		System.out.println("Message Published successfully");
		producer.close();
	}

}
