# Stand Alone App in IntelliJ


### Lets setup our dev environment for an App too!

Clone the examples project from Datastax!

[Spark Build Examples](https://github.com/datastax/SparkBuildExamples)

    git clone git@github.com:datastax/SparkBuildExamples.git
    
This project has sample builds for all combinations of

* OSS / DSE
* Scala / Java
* Sbt / Gradle / Maven

### Load up project in IntelliJ

Copy out the Gradle OSS Example

   mkdir MyApp
   cd MyApp
   cp -r ~/repos/SparkBuildExamples/scala/gradle/oss/ .
   
In IntelliJ new from existing sources


### Lets test out adding a new function and integration test

Goal: Write a function which takes Data from Cassandra and finds
some aggregated statistics.


Start by writing our new integration test.

### Make a new file SummarizeSpec.scala

#### First start out with our boilerplate

    package com.datastax.spark.example

    import com.datastax.spark.connector.cql.CassandraConnector
    import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
    import org.scalatest.{FlatSpec, Matchers}
    import org.apache.spark.sql.cassandra._
    import org.junit.runner.RunWith
    import org.scalatest.junit.JUnitRunner


    @RunWith(classOf[JUnitRunner])
    class SummarizeSpec extends FlatSpec with EmbeddedCassandra with SparkTemplate with Matchers{
      override def clearCache(): Unit = CassandraConnector.evictCache()

      //Sets up CassandraConfig and SparkContext
      useCassandraConfig(Seq(YamlTransformations.Default))
      useSparkConf(defaultConf)

      val connector = CassandraConnector(defaultConf)
      val ksName = "test"
      val tableName = "summarize"

    }
    
#### Next let's setup some data 

     connector.withSessionDo{ session =>
        //Setup a table and keyspaces
        session.execute(s"CREATE KEYSPACE $ksName WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
        session.execute(s"CREATE TABLE $ksName.$tableName (k Int, v Int, PRIMARY KEY (k, v))")


        //Prepare a statement to insert some data
        val ps =
          session
            .prepare(s"INSERT INTO $ksName.$tableName (k,v) VALUES (?,?)")
            .setIdempotent(true)

        // Set all the inserts up
        val futures = for (i <- 1 to 100; j <- 1 to 100) yield {
          session.executeAsync(ps.bind(i: java.lang.Integer, j: java.lang.Integer))
        }
        // Wait for them to complete
        futures.foreach(_.getUninterruptibly())
     }
     
#### Now just a dummy test to make sure this data was loaded

     "Summarizer" should "have data loaded in the test" in {
        sparkSession.read.cassandraFormat(tableName, ksName).load.count() should be (10000)
     }
     
#### Finally add our test for our new functionality

     it should "find the average value of v" in {
        val df = sparkSession.read.cassandraFormat(tableName, ksName).load()
        Utils.average(df, df("v")) should be (51l)
     }
     
#### Next back in WriteRead.scala add the new average function

    object Utils {
      def average(df: DataFrame, col: Column): Long =  {
        df.select(ceil(avg(col)) as "average").collect().head.getLong(0)
      }
    }

   
  