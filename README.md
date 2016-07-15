# Spark
This project contains programs for Spark in Scala launguage .

Topics Covered :     
----------------
Spark Transformations.   
Spark To Cassandra connection and storage.       
Spark To Cassandra CRUD operations.              
Reading data from Cassandra using spark streaming(Cassandra as source).                
Spark Kafka Integration.       
Spark Streaming with Kafka.     
Storing the Spark Streaming data in to HDFS.      
Storing the Spark Streaming data in to Cassandra.       
Spark DataFrames API (Joining 2 data frames,sorting,wild card search,orderBy,Aggregations).         
Spark SQL.      
Spark Hive Context (Loading ORC,txt,parquet data from Hive table ).     
Kafka Producer.     
Kafka Consumer by Spark integration with Kafka.     
Spark File Streaming.     
Spark JDBC Connection.      
Scala Case Class limitations overcoming by using Struct Type.     
Reading CSV,Json,ORC,Parquet data files in Spark.     
Spark Joins.      
Spark Aggregations using DataFrame API.     
Storing data as text,parquet file in to HDFS.     
Integrating Spark with Mangodb.             
Spark Use cases :
----------------
Analysing different kinds of data sets and solving the problem statements.  

  
This document is prepared based on the following BigData Stack.    
---------------------------------------------------------------
HDP 2.4           
Spark 1.5.2          
Scala 2.10.4             
HDFS 2.7             
Kafka 0.9              
Hive 1.2                        
Cassandra 2.2.1                      

------------------------------------------------------------------------------------------------------------------------------------- 

Sample Spark Submit commands for the programs in this blog :             

spark-submit --class com.spark.transformations.examples.Filter --master local[2] /hdp/dev/lib/spark-scala-1.0.jar  

spark-submit --class com.spark.examples.KafkaProducer --master local[2] /hdp/dev/lib/spark-scala-1.0.jar 192.168.19.130:6667 test  

spark-submit --class com.spark.examples.KafkaConsumer --master local[2] /hdp/dev/lib/spark-scala-1.0.jar 192.168.19.130:6667 test

spark-submit --class com.spark.usecases.NamesAnalysis --master local[2] /hdp/dev/lib/spark-scala-1.0.jar            

spark-submit --class com.spark.usecases.OlaDataAnalysis --master local[2] /hdp/dev/lib/spark-scala-1.0.jar        

spark-submit --class com.spark.examples.KafkaConsumerToCassandra --master local[2] /hdp/dev/lib/spark-scala-1.0.jar 192.168.19.130:6667 test                                      

spark-submit --class com.spark.examples.KafkaConsumerToHDFS --master local[2] /hdp/dev/lib/spark-scala-1.0.jar 192.168.19.130:6667 test                         


--------------------------------------------------------------------------------------------------------------------------------------
Kafka commands ::::

cd /usr/hdp/current/kafka-broker/                 
Create a topic:                          
bin/kafka-topics.sh --create --zookeeper 192.168.19.130:2181 --replication-factor 1 --partitions 1 --topic test              
bin/kafka-topics.sh --zookeeper 192.168.19.130:2181 --list                    
Send some messages:               
bin/kafka-console-producer.sh --broker-list 192.168.19.130:6667 --topic test                       
Start a consumer:                    
bin/kafka-console-consumer.sh --zookeeper 192.168.19.130:2181 --topic test --from-beginning                

--------------------------------------------------------------------------------------------------------------------------------------
Cassandra installation :::     
curl -L http://downloads.datastax.com/community/dsc-cassandra-2.2.1-bin.tar.gz | tar xz   

Add Cassandra path as below :::    
sudo vi .bashrc             
export CASSANDRA_HOME=/hdp/dev/dsc-cassandra-2.2.1                   
export PATH=$CASSANDRA_HOME/bin:$PATH                      
source .bashrc                 

Start and Login to Cassandra :::             
>>cassandra -f                                       
>>cqlsh                                 

>>CREATE KEYSPACE IF NOT EXISTS spark_kafka_cassandra WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };     
>>use spark_kafka_cassandra ;                             
>>CREATE TABLE IF NOT EXISTS spark_kafka_cassandra.employee (id int PRIMARY KEY,name VARCHAR, salary int);                

------------------------------------------------------------------------------------------------------------------------------------     

You can reach me for any suggestions/clarifications on  : revanthkumar95@gmail.com                                              
Feel free to share any insights or constructive criticism. Cheers!!                                                           
#Happy Sparking!!!..  


