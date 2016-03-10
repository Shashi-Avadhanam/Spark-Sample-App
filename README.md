# Spark Sample Application

This is sample spark repository for me to learn Spark

Spark example
```
spark-submit --class "SparkMLApp.Activity.ActivityMain" --master local /Users/shashi/code/SparkMLApp/target/SparkMLAppl-1.0-SNAPSHOT.jar /Users/shashi/code/SparkMLApp/data/data2.csv
```
Spark streaming example
```
spark-submit --class "SparkMLApp.Activity.ActivityStreamingMain" --master local /Users/shashi/code/SparkMLApp/target/SparkMLAppl-1.0-SNAPSHOT.jar /Users/shashi/code/SparkMLApp/data
```
Spark SQL example
```
spark-submit --class "SparkMLApp.Activity.ActivitySQLMain" --master local /Users/shashi/code/SparkMLApp/target/SparkMLAppl-1.0-SNAPSHOT.jar /Users/shashi/code/SparkMLApp/data/data2.csv
```
SparkHiveContext example
```
spark-submit --class "SparkMLApp.Activity.ActivityHiveMain" --master local /Users/shashi/code/SparkMLApp/target/SparkMLAppl-1.0-SNAPSHOT.jar
```
Spark Avro example
```
spark-submit --class "SparkMLApp.Activity.ActivityAvroMain" --packages com.databricks:spark-avro_2.11:2.0.1 --master local /Users/shashi/code/SparkMLApp/target/SparkMLAppl-1.0-SNAPSHOT.jar
```
Spark Kafka integration example
start zookeeper
```
zkserver start
```
start kafka
```
kafka-server-start.sh /usr/local/etc/kafka/server.properties
```
run kafka producer
```
java -classpath "/Users/shashi/code/SparkMLApp/target/SparkMLAppl-1.0-SNAPSHOT.jar:/usr/local/Cellar/kafka/0.8.2.2/libexec/core/build/libs/kafka_2.10-0.8.2.2.jar:/usr/local/Cellar/kafka/0.8.2.2/libexec/core/build/dependant-libs-2.10.4/*:/usr/local/Cellar/kafka/0.8.2.2/libexec/clients/build/libs/kafka-clients-0.8.2.2.jar" SparkMLApp.Activity.ActivityKafkaProducer /Users/shashi/code/SparkMLApp/data/data2.csv
```
run spark kafka streaming example
```
spark-submit --class "SparkMLApp.Activity.ActivityKafkaStreamingMain" --jars /usr/local/Cellar/kafka/0.8.2.2/libexec/core/build/libs/kafka_2.10-0.8.2.2.jar,/Users/shashi/.m2/repository/org/apache/spark/spark-streaming-kafka-assembly_2.10/1.6.0/spark-streaming-kafka-assembly_2.10-1.6.0.jar --master local[2] /Users/shashi/code/SparkMLApp/target/SparkMLAppl-1.0-SNAPSHOT.jar 
```

Apache beam example (Google cloud dataflow API with Spark engine)
```
spark-submit \
  --class SparkMLApp.Activity.ActivityGoogleDataflowMain \
  --master local \
  --jars $(echo /Users/shashi/code/SparkMLApp/target/lib/*.jar | tr ' ' ',') \
  /Users/shashi/code/SparkMLApp/target/SparkMLAppl-1.0-SNAPSHOT.jar \
    --inputFile=/Users/shashi/code/SparkMLApp/data/data2.csv  --output=/tmp/out --runner=SparkPipelineRunner --sparkMaster=local
```