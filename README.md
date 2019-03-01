# Application Overview
This application demonstrates the use of spark to process call events from Kafka and produce the results back to kafka and live dashboard.

# Application Architecture
     
                                                                                |------> Kafka Topic
    Kafka Topic ---Call Events---> Spark Streaming ---Calls Processed Events----|
                                                                                |------> Live Dashboard                                                                                                                   
# Version Compatibility

The following tools are dependent on each others version.
Following table has the right versions to be used.

| Tool | Version |
|:-----:|:--------:|
|Kafka |0.10.0 or higher |
|Spark |2.4.0 |
|Scala |2.11.X|

# Application Logic

Step 1: Consume call events from Kafka Topic  
Step 2: Calculate total call duration of a call.  
Step 3: Calculate following metrics per 10 minute sliding window:
   * Total calls disconnected.
   * Average handling time.
   * Average queue time.
   * Average call duration.
   * Total number of repeated customers.

Step 4: Calculate anamolous calls.  
Step 5: Calculate top 5 call reasons.  
Step 6: Join Calls with Product File data  
Step 7: Produce results to kafka topic.  
Step 8: Send the results to live dashboard.


# Deploy

```shell
spark-submit --class "com.hs.spark.streaming.calls.Application" --master spark://spark-master:7077  /root/jars/spark-scala-streaming-application-calls_2.11-0.1.jar
```

# Issues encountered
1. org.apache.kafka - Unresolved Dependencies - No need to add this dependency. It exists in the org.apache.spark.streaming.kafka-10 dependency.
2. Version conflict between spark and scala.
3. Need to create fat jar for spark-submit --> [Helpful Link](http://queirozf.com/entries/creating-scala-fat-jars-for-spark-on-sbt-with-sbt-assembly-plugin)
4. Consumer Record is not serializable. So print on the stream<consumer Records> will not work. [Helpful Link](https://stackoverflow.com/questions/49310214/spark-parallel-stream-object-not-serializable)
5. Kafka Producer initialization/ serialization issue --> [Helpful Link] (https://allegro.tech/2015/08/spark-kafka-integration.html)
