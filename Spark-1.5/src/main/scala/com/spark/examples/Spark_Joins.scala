package com.spark.examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Spark_Joins {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark-Joins").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // Create emp RDD
    val emp = sc.parallelize(Seq((1, "revanth", 10), (2, "dravid", 20), (3, "kiran", 30), (4, "nanda", 35), (5, "kishore", 30)))

    // Create dept RDD
    val dept = sc.parallelize(Seq(("hadoop", 10), ("spark", 20), ("hive", 30), ("sqoop", 40)))

    // Establishing that the third field is to be considered as the Key for the emp RDD
    val manipulated_emp = emp.keyBy(t => t._3)

    // Establishing that the second field need to be considered as the Key for dept RDD
    val manipulated_dept = dept.keyBy(t => t._2)

    // Inner Join
    val join_data = manipulated_emp.join(manipulated_dept)
    
    // Left Outer Join
    val left_outer_join_data = manipulated_emp.leftOuterJoin(manipulated_dept)
    left_outer_join_data.collect().foreach(f => println(f))
    
    // Right Outer Join
    val right_outer_join_data = manipulated_emp.rightOuterJoin(manipulated_dept)
    right_outer_join_data.collect().foreach(f => println(f))
    
    // Full Outer Join
    val full_outer_join_data = manipulated_emp.fullOuterJoin(manipulated_dept)
    full_outer_join_data.collect().foreach(f => println(f))
    
    // Formatting the Joined Data for better understandable (using map)
    val cleaned_joined_data = join_data.map(t => (t._2._1._1, t._2._1._2, t._1, t._2._2._1))

    cleaned_joined_data.collect().foreach(f => println(f))
  }
}