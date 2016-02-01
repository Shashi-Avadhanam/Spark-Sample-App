package SparkMLApp.Activity;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by shashi on 29/01/16.
 */
public class ActivityAvroMain {

    public static void main(String[] args) {
        // Create a Spark Context.
        SparkConf conf = new SparkConf().setAppName("Activity").set("spark.eventLog.enabled", "true");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

// Creates a DataFrame from a specified file
        DataFrame df = sqlContext.read().format("com.databricks.spark.avro")
                .load("src/test/resources/episodes.avro");

// Saves the subset of the Avro records read in
        df.filter("doctor > 5").write()
                .format("com.databricks.spark.avro")
                .save("/tmp/output");

    }
}
