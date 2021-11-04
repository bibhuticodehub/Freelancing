package org.reconciliationframework.copyemrpartitions

import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode

case class EmrLookUp(source_table: String, sql_query: String, target_table: String)

object CopyEMRDataToOnPrem {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("sparkreconframework").master("local").getOrCreate()

    val lookUpDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sparkdb")
      .option("dbtable", "emr_lookup")
      .option("user", "root")
      .option("password", "root@123")
      .load()
    
    import spark.implicits._
    
    val sourceTargetList = lookUpDF.as[EmrLookUp].collectAsList.toSeq
    
    sourceTargetList.forEach{ row =>
      val sqlQuery = row.sql_query
      val targetTable = row.target_table
      
      val sourceDF = spark.read
            .format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/sparkdb")
            .option("dbtable", s"(${sqlQuery}) as t")
            .option("user", "root")
            .option("password", "root@123")
            .load()
            
      sourceDF.write
          .format("jdbc")
          .mode(SaveMode.Append)
          .option("url", "jdbc:mysql://localhost:3306/sparkdb")
          .option("dbtable", s"${targetTable}")
          .option("user", "root")
          .option("password", "root@123")
          .save()
      
    }
  }
}


