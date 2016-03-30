package SparkMLApp.Activity;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.Row;

/**
 * Created by shashi on 25/01/16.
 * usage:
 * rm -r /Users/shashi/code/SparkMLApp/metastore_db
 * sudo spark-submit --class "SparkMLApp.Activity.ActivityHiveMain" --master local /Users/shashi/code/SparkMLApp/target/SparkMLAppl-1.0-SNAPSHOT.jar
 */
public class ActivityHiveMain {

    public static void main(String[] args) {
        // Create a Spark Context.
        SparkConf conf = new SparkConf().setAppName("Activity").set("spark.eventLog.enabled", "true");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc.sc());
        sqlContext.sql("CREATE TABLE IF NOT EXISTS Activity (user STRING, activity STRING,timestamp STRING, xaxis double, yaxis double, zaxis double)");
        sqlContext.sql("LOAD DATA LOCAL INPATH '/Users/shashi/code/SparkMLApp/data/data2.csv' INTO TABLE Activity");

// Queries are expressed in HiveQL.
        System.out.println("Output of table query");
        Row[] results = sqlContext.sql("FROM Activity SELECT user, activity,timestamp, xaxis , yaxis , zaxis").collect();
        for (Row row : results) {
            System.out.println(row.toString());
        }


    }
}
