package kafka.springboot.app.consumer.listener;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import kafka.springboot.app.consumer.model.User;

@Service
public class KafkaConsumer {

	@KafkaListener(topics = "TestTopic", group = "group_json", containerFactory = "userKafkaListenerFactory")
	public void consumeJson(User user) {
		System.out.println("Consumed JSON Message: " + user);
	}


	@KafkaListener(topics = "TestTopic", group = "group_id") public void
	consume(String message) { System.out.println("Consumed message: " + message);
	}

}
