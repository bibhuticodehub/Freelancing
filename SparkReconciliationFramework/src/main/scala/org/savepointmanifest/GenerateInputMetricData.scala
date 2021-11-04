package org.savepointmanifest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.JavaConversions._
import org.apache.spark.sql.expressions.Window


import java.util.concurrent.Executors

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutorService
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import org.apache.spark.sql.SaveMode

case class InputMetricData(Type: String, TaskId: String, RecordsProcesssed: Int, BatchId: String, TimeStamp: Timestamp, ClientType: String)

object GenerateInputMetricData {

  def getCurrentTimeStamp(): Timestamp = {
    val today: Date = Calendar.getInstance.getTime
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now: String = timeFormat.format(today)
    Timestamp.valueOf(now)
  }

  def doParallelInsertMetricData(inputDataObj: InputMetricData)(implicit ec: ExecutionContextExecutorService,spark: SparkSession): Future[Unit] = Future {
    
    val metricId = inputDataObj.TaskId match {
       case "PC_POLICY" => 1
       case "PC_CLAIM" => 2
       case "PC_BILLING" => 3
    }
    
    val sourceId = inputDataObj.Type match {
       case "SavePoint" => 1
       case "Manifest" => 2
    }
    val batchId = inputDataObj.BatchId
    val metricValue = inputDataObj.RecordsProcesssed
    val metricVersion = "V2"
    
    val metricDataSeq = Seq((metricId,sourceId,batchId,metricVersion,metricValue,"20210923","Bibhuti"))
    val metricDataRdd = spark.sparkContext.parallelize(metricDataSeq)
    val metricDF = spark.createDataFrame(metricDataRdd).toDF("MetricId","SourceId","BatchId","MetricVersion","MetricValue","CreatedTimeSTamp","CreatedBy")
    
    metricDF.write
          .format("jdbc")
          .mode(SaveMode.Append)
          .option("url", "jdbc:mysql://localhost:3306/sparkdb")
          .option("dbtable", "metric_data")
          .option("user", "root")
          .option("password", "root@123")
          .save()
  }
  
  def main(args: Array[String]) {
    implicit val spark = SparkSession.builder.appName("sparkreconframework").master("local").getOrCreate()

    import spark.implicits._

    // Create InputMetricData Object for etlaudit_savepoint
    val etlSavePointColumns = Seq(
      "TaskId",
      "RecordsProcesssed",
      "BatchId",
      "Status").map(m => col(m))

    val etlSavePointDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sparkdb")
      .option("dbtable", "etlaudit_savepoint")
      .option("user", "root")
      .option("password", "root@123")
      .load()
      .select(etlSavePointColumns: _*)
      .orderBy(asc("BatchId"))

    val inputDataObjDS = etlSavePointDF.map(row => InputMetricData("SavePoint", row.getString(0), row.getInt(1), row.getString(2), getCurrentTimeStamp, "CDA_CLIENT"))

    val inputDataObjList = inputDataObjDS.collectAsList.toSeq
    
    /** Parallel Data Insertion Happens **/
    
    println("========Data Insertion Thread For Savepoint Starts========")
    val availableProcessor = Runtime.getRuntime.availableProcessors
    implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(availableProcessor))
    val savepointTasks = Future.traverse(inputDataObjList)(inputDataObj => doParallelInsertMetricData(inputDataObj)(ec,spark))
    val completedSavePointInsertion = Await.result(savepointTasks, Duration.Inf)
    println("========Data Insertion Thread For Savepoint Ends========")

    // Create InputMetricData Object for Manifest
    val manifestColumns = Seq(
      "TaskId",
      "RecordsReceived",
      "BatchId").map(m => col(m))

    val manifestDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sparkdb")
      .option("dbtable", "manifest")
      .option("user", "root")
      .option("password", "root@123")
      .load()
      .select(manifestColumns: _*)
      .orderBy(asc("BatchId"))

    val partitionWindow = Window.partitionBy("TaskId").orderBy("BatchId").rowsBetween(Window.unboundedPreceding, Window.currentRow - 1)

    val manifestWithStatusDF = manifestDF.join(etlSavePointDF, manifestDF("BatchId") === etlSavePointDF("BatchId")
      && manifestDF("TaskId") === etlSavePointDF("TaskId"),
      "left").drop(etlSavePointDF("TaskId"))
      .drop(etlSavePointDF("RecordsProcesssed"))
      .drop(etlSavePointDF("BatchId"))
      .withColumn("LagCol", max(when(col("Status") === lit("Success"), col("RecordsReceived"))).over(partitionWindow))
      .withColumn("RecordsProcesssed", when(col("LagCol").isNotNull, col("RecordsReceived") - col("LagCol")).otherwise(col("RecordsReceived")))
    
    val manifestInputDataObjDS = manifestWithStatusDF.map(row => InputMetricData("Manifest", row.getString(0), row.getInt(5), row.getString(2), getCurrentTimeStamp, "CDA_CLIENT"))

    val manifestInputDataObjList = manifestInputDataObjDS.collectAsList.toSeq
    
    println("========Data Insertion Thread For Manifest Starts========")
    val manifestTasks = Future.traverse(manifestInputDataObjList)(inputDataObj => doParallelInsertMetricData(inputDataObj)(ec,spark))
    val completedManifestInsertion = Await.result(manifestTasks, Duration.Inf)      
    println("========Data Insertion Thread For Manifest Ends========")
    ec.shutdown
    println("==========Finishing Main=========")
  }
}