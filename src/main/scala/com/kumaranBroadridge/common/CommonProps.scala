package com.kumaranBroadrige.common

import java.util.Properties
import java.io.FileInputStream
import scala.collection.mutable._
import org.apache.spark.sql.SparkSession
import org.apache.commons.lang.mutable.Mutable
import org.apache.spark.serializer.KryoSerializer


object CommonProps {
  
  case class ConnectionParmater(url: String, connProp: Properties)
  
  val sparkSession = SparkSession.builder()
                          .appName("SparkDB2Testing")
                          .master("local[*]")
                          .getOrCreate()

  
  def db2Credentials(): ConnectionParmater = {
    
    var cred:ConnectionParmater = null
    
    try{
    
      val prop = new Properties()
      prop.load(new FileInputStream("config.properties"))
      val connProperties = new Properties()
      connProperties.put("user", prop.getProperty("DB2.UserName"))
      connProperties.put("password", prop.getProperty("DB2.Password"))
      connProperties.put("driver", prop.getProperty("DB2.Driver"))
      
      cred = new ConnectionParmater(prop.getProperty("DB2.jdbcurl"),connProperties)
    
      cred
      
    }catch{
      case e: Exception => println(e)
      sys.exit(1)
    }
    cred
  }
  
  def tableName(input: String): String = {
    
    try{
      val prop = new Properties()
      prop.load(new FileInputStream("config.properties"))
      
      val tableNames = scala.collection.mutable.Map[String, String]()
      
      tableNames.put("pauni1", prop.getProperty("DB2.PAUNIL"))
      tableNames.put("pap2u1", prop.getProperty("DB2.PAP2U1"))
      tableNames.put("paprc1", prop.getProperty("DB2.PAPRC1"))
      tableNames.put("pap2t1", prop.getProperty("DB2.PAP2T1"))
      tableNames.put("patem1", prop.getProperty("DB2.PATEM1"))
      tableNames.put("pap2v1", prop.getProperty("DB2.PAP2V1"))
      tableNames.put("paxvm1", prop.getProperty("DB2.PAXVM1"))
      tableNames.put("paxln1", prop.getProperty("DB2.PAXLN1"))
      tableNames.put("paxpr1", prop.getProperty("DB2.PAXPR1"))
      tableNames.put("paxmy1", prop.getProperty("DB2.PAXMY1"))
      tableNames.put("paplt1", prop.getProperty("DB2.PAPLT1"))
      tableNames.put("papdt1", prop.getProperty("DB2.PADPT1"))
      tableNames.put("paunh1", prop.getProperty("DB2.PAUNH1"))
      tableNames.put("paprh1", prop.getProperty("DB2.PAPRH1"))
      
      tableNames(input)
            
    }catch{
      case e: Exception => println(e);null
    }
    
  }
    
}