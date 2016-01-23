package SparkMLApp.Activity;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ActivityKafkaProducer {
	
	  public static void main(String[] args) throws IOException, InterruptedException 
	  {   
	        Properties props = new Properties();
	        props.put("metadata.broker.list", "localhost:9092");
	        String TOPIC = "activityevent";
	        ProducerConfig config = new ProducerConfig(props);
	        Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(config);
	        BufferedReader activitybr = new BufferedReader(new FileReader(args[0]));
	        String line;
	        while ((line = activitybr.readLine()) != null) {
	        	System.out.println(line);
	           KeyedMessage<byte[], byte[]> data = new KeyedMessage<byte[], byte[]>(TOPIC, line.getBytes());
                producer.send(data);
                Thread.sleep(1000);
	        }
	        activitybr.close();
	        producer.close();
	    }
	  

}
