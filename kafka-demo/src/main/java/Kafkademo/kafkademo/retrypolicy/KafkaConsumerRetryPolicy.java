package Kafkademo.kafkademo.retrypolicy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaConsumerRetryPolicy {
	public static void main(String[] args) {
		SpringApplication.run(KafkaConsumerRetryPolicy.class, args);
	}

}
