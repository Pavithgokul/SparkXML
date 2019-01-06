package com.spark.utils

import org.apache.spark.sql.SparkSession

trait SparkUtils {
  
  def spark : SparkSession = {
    val sparkSession = SparkSession.builder()
                                   .appName("Spark XMl Learning")
                                   .master("local")
                                   .getOrCreate()
                                   
    sparkSession
  }
  
}