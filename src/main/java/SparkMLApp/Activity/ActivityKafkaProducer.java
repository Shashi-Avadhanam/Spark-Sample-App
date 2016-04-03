package SparkMLApp.Activity;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by shashi on 25/01/16.
 * usage:
 * zkserver start
 kafka-server-start.sh /usr/local/etc/kafka/server.properties
 java -classpath "/Users/shashi/code/SparkMLApp/target/SparkMLAppl-1.0-SNAPSHOT.jar:/usr/local/Cellar/kafka/0.8.2.2/libexec/core/build/libs/kafka_2.10-0.8.2.2.jar:/usr/local/Cellar/kafka/0.8.2.2/libexec/core/build/dependant-libs-2.10.4/*:/usr/local/Cellar/kafka/0.8.2.2/libexec/clients/build/libs/kafka-clients-0.8.2.2.jar" SparkMLApp.Activity.ActivityKafkaProducer /Users/shashi/code/SparkMLApp/data/data2.csv
 */


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
