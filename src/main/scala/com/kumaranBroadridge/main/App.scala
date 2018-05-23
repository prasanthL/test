package com.kumaranBroadridge.main

import org.apache.spark.sql.SparkSession
import com.kumaranBroadrige.common.CommonProps

object App extends App {
  
  if (args.length == 0){
    println("Please provide the argument")
  }
  
  val value1 = args(0)
  val value2 = args(1)
  
  println(s"Please find the input value ************ $value1 & $value2")
  
  val spark = CommonProps.sparkSession
  
  val input = spark.sparkContext.parallelize(List(1,2,3,45,6,7,89))
  
  val output = input.map { x => x+1 }
  
  output.collect().foreach { println }
  
}
