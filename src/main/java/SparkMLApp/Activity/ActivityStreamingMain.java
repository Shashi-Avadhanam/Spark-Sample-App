package SparkMLApp.Activity;

import scala.Tuple2;
//import java.util.Comparator;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import com.google.common.base.Optional;

public class ActivityStreamingMain {

	  private static Function2<Double, Double, Double> SUM_REDUCER = (a, b) -> a + b;
	 private static final Duration WINDOW_LENGTH = new Duration(30 * 1000);
	  
	  private static final Duration SLIDE_INTERVAL = new Duration(10 * 1000);
	  private static final AtomicLong runningCount = new AtomicLong(0);
	  private static final AtomicLong runningSum = new AtomicLong(0);
	  private static final AtomicLong runningMin = new AtomicLong(Long.MAX_VALUE);
	  private static final AtomicLong runningMax = new AtomicLong(Long.MIN_VALUE);
	  
	  private static Function2<List<Double>, Optional<Double>, Optional<Double>> 
	     COMPUTE_RUNNING_SUM = (nums, current) -> {
	       double sum = current.or(0D);
	       for (double i : nums) {
	         sum += i;
	       }
	       return Optional.of(sum);
	     };

  public static void main(String[] args) {
    // Create a Spark Context.
    SparkConf conf = new SparkConf().setAppName("Activity").set("spark.eventLog.enabled", "true");;
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext jssc = new JavaStreamingContext(sc,SLIDE_INTERVAL);   
    jssc.checkpoint("/tmp/activitystream");
  
    if (args.length == 0) {
      System.out.println("Must specify an activity file.");
      System.exit(-1);
    }
    String ActiityFile = args[0];
    //JavaPairInputDStream<LongWritable, Text> activitydstream=jssc.fileStream(ActiityFile,LongWritable, Text, TextInputFormat);
    JavaDStream<String> activitydatastream= jssc.textFileStream(ActiityFile);
    //activitydatastream.print();
    JavaDStream<Activity> ActivityEntryDStream = activitydatastream.map(Activity::parseFromLine);
    JavaDStream<Activity> windowDStream = ActivityEntryDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL);
    windowDStream.foreachRDD( ActivityEntry -> { 
    	 if (ActivityEntry.count() == 0) {
    		    System.out.println("No Activities in this time interval");
    		    return null;
    		  }
    	  runningSum.getAndAdd(Double.doubleToLongBits(ActivityEntry.map(Activity::getXaxis).reduce(SUM_REDUCER)));
          runningCount.getAndAdd(ActivityEntry.count());
          runningMin.set(Double.doubleToLongBits(Math.min(runningMin.get(), ActivityEntry.map(Activity::getXaxis).min(Comparator.naturalOrder()))));
          runningMax.set(Double.doubleToLongBits(Math.max(runningMax.get(), ActivityEntry.map(Activity::getXaxis).max(Comparator.naturalOrder()))));
 JavaRDD<Double> xaxisstat =
    		       ActivityEntry.map(Activity::getXaxis).cache();
    		    System.out.println(String.format("X axis Count: %s, Avg: %s, Min: %s, Max: %s",
    		    		xaxisstat.count(),
    		    		xaxisstat.reduce(SUM_REDUCER) / xaxisstat.count(),
    		    		xaxisstat.min(Comparator.naturalOrder()),
    		    		xaxisstat.max(Comparator.naturalOrder())));
    		    System.out.println(String.format("Running X axis Count: %s, Avg: %s, Min: %s, Max: %s",
    		    		runningCount.get(),
    		    		runningSum.get() / runningCount.get(),
    		    		runningMin.get(),
    		    		runningMax.get()));
    		    return null;
    });
    
 
JavaPairDStream<String, Double> UserXaxisDStream = ActivityEntryDStream.mapToPair(s -> new Tuple2<String,Double>(s.getUser(),s.getXaxis())).reduceByKey(SUM_REDUCER).updateStateByKey(COMPUTE_RUNNING_SUM);
UserXaxisDStream.foreachRDD(rdd -> {
    System.out.println("Running counts for each key: " + rdd.take(100));
    return null;
}
);
    
/*    JavaPairDStream<String, Double> UserXaxisDStream =
      ActivityEntryDStream.map( new PairFunction<Activity,
      String,Double>() {
      public Tuple2<String, Double> call( Activity activity) {
      return new Tuple2(activity.getUser(),activity.getXaxis()) ; } });*/
     
		jssc.start();
		jssc.awaitTermination();
// jssc.close();
    sc.stop();
    sc.close();
  }
}
