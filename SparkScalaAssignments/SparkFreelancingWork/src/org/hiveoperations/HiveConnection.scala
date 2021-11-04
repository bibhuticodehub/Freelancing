package org.hiveoperations

import org.apache.spark.sql.SparkSession
import org.reconciliationframework.helper.DatabaseContextUtils
import org.apache.spark.sql.SaveMode

object HiveConnection {

  def main(args: Array[String]) {

    val dbName = "sparkdb"
    val externalLocation = "/usr/hive/data/"
    val outputTableName = "players"
    val hiveLocation = externalLocation + outputTableName

    val spark = SparkSession
      .builder()
      .master("local")
      .config("spark.sql.warehouse.dir", "/usr/hive/warehouse")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.sql("select * from sparkdb.countries")

    val dataSeq = Seq((100, "Sachin", 45), (101, "Sehwag", 42), (102, "Sourav", 46))
    val dataRdd = spark.sparkContext.parallelize(dataSeq)
    val sparkDF = spark.createDataFrame(dataRdd).toDF("id", "name", "age")

//    sparkDF.write.format("parquet").mode("overwrite").saveAsTable(dbName + "." + outputTableName)
    
    sparkDF.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", hiveLocation)
      .saveAsTable(dbName + "." + outputTableName)

    println("Data written Successfully")

  }
}