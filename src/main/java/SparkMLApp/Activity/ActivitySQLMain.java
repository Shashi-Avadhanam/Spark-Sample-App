package SparkMLApp.Activity;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by shashi on 25/01/16.
 * usage:
 * spark-submit --class "SparkMLApp.Activity.ActivitySQLMain" --master local /Users/shashi/code/SparkMLApp/target/SparkMLAppl-1.0-SNAPSHOT.jar /Users/shashi/code/SparkMLApp/data/data2.csv
 */

public class ActivitySQLMain {


  public static void main(String[] args) {
    // Create a Spark Context.
    SparkConf conf = new SparkConf().setAppName("Activity").set("spark.eventLog.enabled", "true");;
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
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
       Lines.map(Activity::parseFromLine);
    
    DataFrame schemaActivityEntry = sqlContext.createDataFrame(ActivityEntry, Activity.class);
    schemaActivityEntry.registerTempTable("ActivityEntry");
    
    DataFrame xaxisstat = sqlContext.sql("SELECT avg(xaxis) ,min(xaxis),max(xaxis) FROM ActivityEntry");
    xaxisstat.show();

//    System.out.println(String.format("X axis Avg: %s, Min: %s, Max: %s",
//    		xaxisstat.reduce(SUM_REDUCER) / xaxisstat.count(),
//    		xaxisstat.min(Comparator.naturalOrder()),
//    		xaxisstat.max(Comparator.naturalOrder())));

    // Stop the Spark Context before exiting.
    sc.stop();
    sc.close();
  }
}
