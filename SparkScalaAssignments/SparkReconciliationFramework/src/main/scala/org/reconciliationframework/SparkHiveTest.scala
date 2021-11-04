package org.reconciliationframework

import org.apache.spark.sql.SparkSession

object SparkHiveTest {
  
  def main(args:Array[String]) {
    val spark = SparkSession
      .builder()
//      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()
     
    val sourceDefinitionDF = spark.table("source_definition")
    sourceDefinitionDF.show()
  }
}