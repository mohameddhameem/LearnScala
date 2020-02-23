package com.nus


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._

object NoSQL_Examples {
  def main(args: Array[String]){
    println("Read from Customer Database");
    val inputFilePath = "file:/home/cloudera/Dhameem/Customer.csv"
    val conf = new SparkConf().setAppName("CustomerData").setMaster("local[2]")
    val context = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Test").getOrCreate()
    
    import spark.implicits._
    
    val df = spark.read
              .option("inferschema","true")
              .option("header","true")
              .csv(inputFilePath)
     df.show()
     df.printSchema()
     //Order by CustomerName
     df.orderBy("CustomerName").show()
     //Order by multiple column
     df.orderBy("MemberCategory","CustomerName").show()
     
     /////
     //Selective Retrieval
     println("Selective Retrieval")
     df.filter("MemberCategory = 'A'").show()
     
     //Multiple Filter
     df.filter("MemberCategory = 'A' AND CustomerName LIKE 'T%'").show()
     
     //Lets try to combine the functions
     df.filter("MemberCategory = 'A'").orderBy("CustomerName").show(30,false)
     
     //Aggregation & Simple Statistical Query Examples
     
     val count = df.filter("MemberCategory = 'A'").count()
     println("Total Objects --> " + count)
     
     //Total amount earned
     val intot = df.agg(sum("AmountSpent"))
     println(intot.first().get(0))
     
     //Getting the sum of a field with conditions on the rows to use
     val tot = df.filter("MemberCategory = 'A'").agg(sum("AmountSpent")).first().get(0)
     println("Total ->" + tot)
     
     //Return selected fields
     df.select(df("CustomerID"), df("CustomerName"), df("Age")).show(200, false)
     
     //Complex & Further Statistical Queries
     df.groupBy("MemberCategory").sum("AmountSpent").show(200,false)
     
     df.groupBy("MemberCategory", "ContactTitle").sum("AmountSpent").orderBy("MemberCategory","ContactTitle").show(200,false)
     
     println("Roll up")
     //RollUp function instead of GroupBy
     df.rollup("MemberCategory", "ContactTitle")
         .sum("AmountSpent")
         .orderBy("MemberCategory","ContactTitle").show(200,false)
     
     println("Cube functions")
     //RollUp function instead of GroupBy
     df.cube("MemberCategory", "ContactTitle")
         .sum("AmountSpent")
         .orderBy("MemberCategory","ContactTitle").show(200,false)
     
     //Standard Deviation
        val std = df.agg(stddev_pop("AmountSpent")).first().get(0)
        println(std)
        
        //Skewness of a field
        val skw = df.agg(skewness("AmountSpent")).first().get(0)
        println(skw)
        
        //Describe the dataset
        df.describe().show()
  }
}