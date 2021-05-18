package com.example.kafka.springbootkafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer { 
	public static final String TOPIC= "Demo";
	public static void main(String[] args) throws Exception{

		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "1"); //all
		//props.put("min.insync.replicas","1");
		//props.put("enable.idempotence", true);
		props.put("retries", 0);
		props.put("linger.ms", 1);//props.put("batch.size", 16384);
		props.put("buffer.memory", 33554432);
		//props.put("retention.ms",3000);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		for(int i = 0; i < 5; i++)
		{
			producer.send(new ProducerRecord<String, String>(TOPIC,"key-"+Integer.toString(i), Integer.toString(i)));
			System.out.println(TOPIC+"==>"+i+"published");
		}
		System.out.println("Messages published successfully");
		producer.close();
	}
}