package com.spark.main

import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType


object MainApp extends App {
  
  val sparkSession = SparkSession.builder().appName("Spark XML").master("local").getOrCreate()
  
  val customSchema =new  StructType().add("from", StringType, true)
                                     .add("to", StringType, true)
                                     .add("heading", StringType, true)
                                     .add("body", StringType, true)
  
  val inputDF = sparkSession.read.option("rowTag", "note").schema(customSchema).xml("/home/hadoop-master/sample.xml")
  
  inputDF.show()
  
  sparkSession.close()
  
}