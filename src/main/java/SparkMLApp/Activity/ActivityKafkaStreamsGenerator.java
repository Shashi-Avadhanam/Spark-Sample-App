package SparkMLApp.Activity;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;


/**
 * Created by shashi on 25/01/16.
 * usage:
 * zkserver start
 kafka-server-start.sh /usr/local/etc/kafka/server.properties
 java -classpath $(echo /Users/shashi/code/SparkMLApp/target/lib/*.jar /Users/shashi/code/SparkMLApp/target/*.jar /usr/local/Cellar/kafka/0.8.2.2/libexec/core/build/dependant-libs-2.10.4/* | tr ' ' ':') SparkMLApp.Activity.ActivityKafkaStreamsGenerator
 */

public class ActivityKafkaStreamsGenerator {
	
	  public static void main(String[] args) throws IOException, InterruptedException 
	  {   
	        Properties props = new Properties();
	        props.put("metadata.broker.list", "localhost:9092");
	        String TOPIC = "activityevent";
	        ProducerConfig config = new ProducerConfig(props);
	        Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(config);
	        String line;
		    String user= "Shashi";
            String activ= "walking";
            int xaxis;
            int yaxis;
            int zaxis;
            String Timestamp;
            Random randomGenerator = new Random();
            while (true) {
                Date date=new Date();
                Timestamp = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(date.getTime());
                xaxis=randomGenerator.nextInt(30000);
                yaxis=randomGenerator.nextInt(30000);
                zaxis=randomGenerator.nextInt(30000);
                line = user + "," + activ + "," + Timestamp + "," + xaxis + "," + yaxis + "," + zaxis;
	        	System.out.println(line);
	           KeyedMessage<byte[], byte[]> data = new KeyedMessage<byte[], byte[]>(TOPIC, line.getBytes());
                producer.send(data);
                Thread.sleep(10000);
                if (xaxis == 0) break;
	        }
	        producer.close();
	    }
	  

}
