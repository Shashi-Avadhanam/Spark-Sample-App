package SparkMLApp.Activity;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by shashi on 25/01/16.
 * usage:
 * noglob spark-submit --class "SparkMLApp.Activity.EventTimeWindowPattern" \
 --jars /usr/local/Cellar/kafka/0.8.2.2/libexec/core/build/libs/kafka_2.10-0.8.2.2.jar,/Users/shashi/.m2/repository/org/apache/spark/spark-streaming-kafka-assembly_2.10/1.6.0/spark-streaming-kafka-assembly_2.10-1.6.0.jar \
 --master local[2] /Users/shashi/code/SparkMLApp/target/SparkMLAppl-1.0-SNAPSHOT.jar
 */

public class EventTimeWindowPattern {

	  private static Function2<Double, Double, Double> SUM_REDUCER = (a, b) -> a + b;
	  
	  private static final Duration STREAM_INTERVAL = new Duration(60 * 1000);

    public static class WithTimestamp<T> extends Tuple2<T, Long> {
        WithTimestamp(T val, Long timestamp) {
            super(val, timestamp);
        }

        T val() {
            return _1();
        }

        Long timestamp() {
            return _2();
        }

        public static <T> WithTimestamp<T> create(T val, Long timestamp) {
            return new WithTimestamp<T>(val, timestamp);
        }
    }

    public static long convertMillsecs(String indate) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date date = sdf.parse(indate);
        return (date.getTime());
    }

  public static void main(String[] args) {
    // Create a Spark Context.
    SparkConf conf = new SparkConf().setAppName("Activity").set("spark.eventLog.enabled", "true");;
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext jssc = new JavaStreamingContext(sc,STREAM_INTERVAL);
    String TOPIC = "activityevent";
    String zkQuorum="localhost:2181";
    String group="1";
    Map<String, Integer> topicMap = new HashMap<String, Integer>();
    topicMap.put(TOPIC, 1);


    JavaPairReceiverInputDStream<String, String> messages =
            KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);
    //messages.print();
    JavaDStream<String> activitydatastream= messages.map(new Function<Tuple2<String, String>, String>() {
        @Override
        public String call(Tuple2<String, String> tuple2) {
          return tuple2._2();
        }
      });

      final Long teamWindowDurationMs = Durations.minutes(1).milliseconds();
    JavaDStream<Activity> ActivityEntryDStream = activitydatastream.map(Activity::parseFromLine);
      JavaPairDStream<WithTimestamp<String>,Double> ActivityWindowDStream= ActivityEntryDStream.mapToPair(windows -> new Tuple2<>(
            WithTimestamp.create(
                    windows.getActivity(),
                    // Apply Fixed Window by rounding the timestamp down to the nearest
                    // multiple of the window size
                    (convertMillsecs(windows.getTimestamp()) / teamWindowDurationMs) * teamWindowDurationMs),
            windows.getXaxis())).reduceByKey(SUM_REDUCER);

      ActivityWindowDStream.print();

      jssc.start();
    jssc.awaitTermination();
// jssc.close();
    sc.stop();
    sc.close();
  }
}
