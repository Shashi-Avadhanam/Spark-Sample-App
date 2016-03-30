package SparkMLApp.Activity;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by shashi on 29/01/16.
 * Usage:
 * python /Users/shashi/code/SparkMLApp/src/main/resources/csv2avro.py /Users/shashi/code/SparkMLApp/data/data3.csv /Users/shashi/code/SparkMLApp/data/data3.avro --dialect excel
 * python /Users/shashi/code/SparkMLApp/src/main/resources/read.py  /Users/shashi/code/SparkMLApp/data/data3.avro
 * spark-submit \
 --class "SparkMLApp.Activity.ActivityAvroMain" \
 --jars $(echo /Users/shashi/code/SparkMLApp/target/lib/*.jar | tr ' ' ',') \
 --master local /Users/shashi/code/SparkMLApp/target/SparkMLAppl-1.0-SNAPSHOT.jar
 *
 */
public class ActivityAvroMain {

    public static void main(String[] args) {
        // Create a Spark Context.
        SparkConf conf = new SparkConf().setAppName("Activity").set("spark.eventLog.enabled", "true");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

// Creates a DataFrame from a specified file
        DataFrame df = sqlContext.read().format("com.databricks.spark.avro")
                .load("/Users/shashi/code/SparkMLApp/data/data3.avro");

// Saves the subset of the Avro records read in
        df.filter("xaxis > 0").write()
                .format("com.databricks.spark.avro")
                .save("/tmp/output");

    }
}
