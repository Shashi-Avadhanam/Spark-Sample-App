package SparkMLApp.Activity;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import java.util.Comparator;
import java.util.List;

/**
 * Created by shashi on 25/01/16.
 * usage:
 * spark-submit \
 --class "SparkMLApp.Activity.ActivityMain" \
 --master local \
 /Users/shashi/code/SparkMLApp/target/SparkMLAppl-1.0-SNAPSHOT.jar /Users/shashi/code/SparkMLApp/data/data2.csv
 */

public class ActivityMain {
  private static Function2<Double, Double, Double> SUM_REDUCER = (a, b) -> a + b;

  public static void main(String[] args) {
    // Create a Spark Context.
    SparkConf conf = new SparkConf().setAppName("Activity").set("spark.eventLog.enabled", "true");;
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Load the text file into Spark.
    if (args.length == 0) {
      System.out.println("Must specify an access logs file.");
      System.exit(-1);
    }
    String ActiityFile = args[0];
    JavaRDD<String> Lines = sc.textFile(ActiityFile);

    // Convert the text lines to Activity objects and cache them
    //   since multiple transformations and actions will be called on that data.
    JavaRDD<Activity> ActivityEntry =
       Lines.map(Activity::parseFromLine).cache();

    // Calculate statistics for x axis
    JavaRDD<Double> xaxisstat =
       ActivityEntry.map(Activity::getXaxis).cache();
    System.out.println(String.format("X axis Avg: %s, Min: %s, Max: %s",
    		xaxisstat.reduce(SUM_REDUCER) / xaxisstat.count(),
    		xaxisstat.min(Comparator.naturalOrder()),
    		xaxisstat.max(Comparator.naturalOrder())));

    // Compute Counts by Activity
    List<Tuple2<String, Double>> AcitivityCount =
    		ActivityEntry.mapToPair(entry -> new Tuple2<>(entry.getActivity(), 1D))
            .reduceByKey(SUM_REDUCER)
            .take(100);
    System.out.println(String.format("Activity counts: %s", AcitivityCount));

    // Stop the Spark Context before exiting.
    sc.stop();
    sc.close();
  }
}
