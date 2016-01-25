package SparkMLApp.Activity;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.Row;

/**
 * Created by shashi on 25/01/16.
 */
public class ActivityHiveMain {

    public static void main(String[] args) {
        // Create a Spark Context.
        SparkConf conf = new SparkConf().setAppName("Activity").set("spark.eventLog.enabled", "true");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc.sc());
        sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
        sqlContext.sql("LOAD DATA LOCAL INPATH '/usr/local/Cellar/apache-spark/1.6.0/libexec/examples/src/main/resources/kv1.txt' INTO TABLE src");

// Queries are expressed in HiveQL.
        System.out.println("Output of table query");
        Row[] results = sqlContext.sql("FROM src SELECT key, value").collect();               g
        for (Row row : results) {
            System.out.println(row.toString());
        }


    }
}
