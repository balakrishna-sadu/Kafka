package kafka.springboot.app.producer.controller;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import kafka.springboot.app.producer.model.User;

@RestController
@RequestMapping("kafka")
public class UserController {

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplate;
    
    @Autowired
    private KafkaProducer<String, User> producer;
    
    private static final String TOPIC = "TestTopic";

    
    
    @PostMapping("/publish/{name}")
    public String postName(@PathVariable("name") final String name) {
        kafkaTemplate.send(TOPIC, new User(name, "Technology", 12000L));
        return "Published successfully";
    }
   
    
    @PostMapping("test1") //with producer record without partition specifying
    public String publishDefualtUser() {
    	ProducerRecord<String, User> record = new ProducerRecord<String, User>(TOPIC, new User("Krish","BE",100L));
    	producer.send(record);
    	return "Test-1 method Executed Successfully";
    }
    @PostMapping(value="test2",consumes = "application/json") //with producer record without partition specifying but with dynamic User Object
    public String publishUser(@RequestBody User user) {
    	System.out.println(user);
    	ProducerRecord<String, User> record = new ProducerRecord<String, User>(TOPIC, user);
    	producer.send(record);
    	return "Test-2 method Executed Successfully";
    }
    
    
    @PostMapping("partition") //with parition and key
    public String partitionPublishing(@RequestParam("n") int n) {
    	ProducerRecord<String, User> record = new ProducerRecord<String, User>(TOPIC, n, "ABC123", new User("Mahesh","Fullstack",200L));
    	producer.send(record);
    	return "Partition Method Executed Successfully";
    }
    
    
    @PostMapping("key")//with key only to check whether it going to relevant partition or not
    public String keyPartitionCheck() {
    	ProducerRecord<String, User> record = new ProducerRecord<String, User>(TOPIC, "ABC123", new User("Gikki","FullStack",360L));
    	producer.send(record);
    	return "Key Partition Mapping method Executed Successfully"; //Executing but not storing by key-partition mapping
    }
}
