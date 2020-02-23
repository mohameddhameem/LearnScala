package com.nus

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._

object ScalaStarter2 {
  def main(args: Array[String]){
    val inputCustomerFilePath = "file:/home/cloudera/Dhameem/Customer.csv"
    val inputCountryFilePath = "file:/home/cloudera/Dhameem/Country.csv"
    val conf = new SparkConf().setAppName("CustomerData").setMaster("local[2]")
    val context = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Test").config("x","y").getOrCreate()
    
    import spark.implicits._
    
    val df1 = spark.read.option("header", "true")
    .option("inferSchema","true").csv(inputCustomerFilePath)
    val df2 = spark.read.option("header", "true")
    .option("inferSchema","true").csv(inputCountryFilePath)
    
    val df_join = df1.join(df2, "CountryCode")
    df_join.show(10,false)
    
    df_join.select(df_join("CustomerID"), df_join("CustomerName"),
        df_join("CountryCode"), df_join("CountryName"), df_join("Currency"),
        df_join("TimeZone")).show(300,false)
        
    
  }
}