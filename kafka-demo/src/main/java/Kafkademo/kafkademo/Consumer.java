package Kafkademo.kafkademo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.stereotype.Service;
@Service
@EnableKafka
public class Consumer {
	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

		String bootstrap_servers = "127.0.0.1:9092";
		String group_id = "group_1";
		String topic = "demo-kafka";

		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList(topic));

		ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3*1000));
		for(ConsumerRecord<String, String> record : records) {
			System.out.println("\nKey : "+record.key()+" Value : "+record.value());
			System.out.println("Parition : "+record.partition()+",Offset : "+record.offset());
		}	
	}
}
