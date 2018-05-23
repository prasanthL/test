package com.kumaranBroadridge.main

import org.apache.spark.sql.SparkSession

object SamleMain extends App{
  
  val spark = SparkSession.builder
                          .appName("Job 2")
                          .getOrCreate()
                          
  val input = spark.sparkContext.parallelize(List(1,8,4,78,90))
  
  val output = input.map { x => x*2 }
  
  output.collect().foreach { x => println(x)}
  
  spark.close()
}