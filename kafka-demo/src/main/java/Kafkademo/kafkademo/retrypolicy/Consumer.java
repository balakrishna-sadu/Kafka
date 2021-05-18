package Kafkademo.kafkademo.retrypolicy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
@Service
public class Consumer {
	static Logger log = LoggerFactory.getLogger(Consumer.class);

	@KafkaListener(topics = {"hello-world"})
	public <PackageInfoEvent> void packagesListener(ConsumerRecord<String,PackageInfoEvent> packageInfoEvent){
	    log.info("Received event to persist packageInfoEvent :{}", packageInfoEvent.value());
	}
}
