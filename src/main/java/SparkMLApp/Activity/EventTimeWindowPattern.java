package SparkMLApp.Activity;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class EventTimeWindowPattern {

	  private static Function2<Double, Double, Double> SUM_REDUCER = (a, b) -> a + b;
	 private static final Duration WINDOW_LENGTH = new Duration(30 * 1000);
	  
	  private static final Duration SLIDE_INTERVAL = new Duration(60 * 1000);

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
    JavaStreamingContext jssc = new JavaStreamingContext(sc,SLIDE_INTERVAL);      
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

      final Long allowedLatenessMs = Durations.minutes(5).milliseconds();
      final Long teamWindowDurationMs = Durations.minutes(1).milliseconds();
    JavaDStream<Activity> ActivityEntryDStream = activitydatastream.map(Activity::parseFromLine);
      ActivityEntryDStream.mapToPair(windows -> new Tuple2<>(
            WithTimestamp.create(
                    windows.getActivity(),
                    // Apply Fixed Window by rounding the timestamp down to the nearest
                    // multiple of the window size
                    (convertMillsecs(windows.getTimestamp()) / teamWindowDurationMs) * teamWindowDurationMs),
            windows.getXaxis())).print();

              //foreachRDD(ActivityWindows -> {
       // ActivityWindows.collect();
    //});
      jssc.start();
    jssc.awaitTermination();
// jssc.close();
    sc.stop();
    sc.close();
  }
}
