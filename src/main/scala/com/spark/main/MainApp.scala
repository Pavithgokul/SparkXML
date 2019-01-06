package com.spark.main

import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import com.spark.utils.SparkUtils


object MainApp extends App with SparkUtils{
  
  val customSchema =new  StructType().add("from", StringType, true)
                                     .add("to", StringType, true)
                                     .add("heading", StringType, true)
                                     .add("body", StringType, true)
  
  val inputDF = spark.read.option("rowTag", "note").schema(customSchema).xml("/home/hadoop-master/sample.xml")
  
  inputDF.show()
  
  spark.close()
  
}