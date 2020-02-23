package com.nus

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._

object mysql_read {
  def main(args: Array[String]){
    val producerPath = "file:/home/cloudera/Dhameem/Producers.json"
        val confx = new SparkConf().setAppName("ProducerData").setMaster("local[2]")
        val contextx = new SparkContext(confx)
    val sparky = SparkSession.builder.appName("Test").config("x","y").getOrCreate()
    
    import sparky.implicits._
    val sqlContext = new SQLContext(contextx)
    
    val dfMovies = sqlContext.load("jdbc", Map("jdbc:mysql://localhost/VideoShop?user=root&password=cloudera","dbtable" -> "Movies"))
    
    
  }
}