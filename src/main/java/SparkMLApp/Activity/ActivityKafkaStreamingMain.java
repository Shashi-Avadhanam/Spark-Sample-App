package SparkMLApp.Activity;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class ActivityKafkaStreamingMain {

	  private static Function2<Double, Double, Double> SUM_REDUCER = (a, b) -> a + b;
	 private static final Duration WINDOW_LENGTH = new Duration(30 * 1000);
	  
	  private static final Duration SLIDE_INTERVAL = new Duration(10 * 1000);

  public static void main(String[] args) {
    // Create a Spark Context.
    SparkConf conf = new SparkConf().setAppName("Activity").set("spark.eventLog.enabled", "true");;
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext jssc = new JavaStreamingContext(sc,SLIDE_INTERVAL);      
    String TOPIC = "activityevent";
    String zkQuorum="localhost:2181";
    String group="1";
    Map<String, Integer> topicMap = new HashMap<String, Integer>();
    topicMap.put(TOPIC, 1);
    
    JavaPairReceiverInputDStream<String, String> messages =
            KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);
    messages.print();
    JavaDStream<String> activitydatastream= messages.map(new Function<Tuple2<String, String>, String>() {
        @Override
        public String call(Tuple2<String, String> tuple2) {
          return tuple2._2();
        }
      });
    JavaDStream<Activity> ActivityEntryDStream = activitydatastream.map(Activity::parseFromLine);
    ActivityEntryDStream.print();
    JavaDStream<Activity> windowDStream = ActivityEntryDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL);
    windowDStream.foreachRDD( ActivityEntry -> { 
    	 if (ActivityEntry.count() == 0) {
    		    System.out.println("No Activities in this time interval");
    		    return null;
    		  }
    	 ActivityEntry.collect();
 JavaRDD<Double> xaxisstat =
    		       ActivityEntry.map(Activity::getXaxis).cache();
    		    System.out.println(String.format("X axis Avg: %s, Min: %s, Max: %s",
    		    		xaxisstat.reduce(SUM_REDUCER) / xaxisstat.count(),
    		    		xaxisstat.min(Comparator.naturalOrder()),
    		    		xaxisstat.max(Comparator.naturalOrder())));
    		    return null;
    });

    jssc.start();              
    jssc.awaitTermination();
// jssc.close();
    sc.stop();
    sc.close();
  }
}
