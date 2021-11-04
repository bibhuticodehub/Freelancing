package org.reconciliationframework

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

object SparkReconciliationFramework {

  def insertReconMetricLog(spark: SparkSession, metricComMapIdscsv: String): Unit = {

    val reconLogSchema = new StructType()
      .add(StructField("MetricComparisionMapId", IntegerType, true))
      .add(StructField("MetricBatch", StringType, true))
      .add(StructField("MetricVersion", StringType, true))
      .add(StructField("SourceId", IntegerType, true))
      .add(StructField("SourceValue", IntegerType, true))
      .add(StructField("TargetId", IntegerType, true))
      .add(StructField("TargetValue", IntegerType, true))
      .add(StructField("ThresholdPercentage", IntegerType, true))
      .add(StructField("DeviationPercentage", IntegerType, true))
      .add(StructField("ReconStatus", StringType, true))

   import spark.implicits._
      
    val metricCompMapColumns = Seq(
      "MetricComparisionMapId",
      "ComparisionId",
      "MetricId",
      "ThresholdPercentage").map(m => col(m))

    val metricComMapDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sparkdb")
      .option("dbtable", "metric_comparision_map")
      .option("user", "root")
      .option("password", "root@123")
      .load().select(metricCompMapColumns: _*)

    val compDefColumns = Seq(
      "ComparisionId",
      "SourceId",
      "TargetId").map(m => col(m))

    val comDefinitionDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sparkdb")
      .option("dbtable", "comparision_definition")
      .option("user", "root")
      .option("password", "root@123")
      .load().select(compDefColumns: _*)
    
    val comDefUpdated =  comDefinitionDF.withColumn("Source_Target_Array",array(col("SourceId"), col("TargetId")))
    val filterDF = comDefUpdated.select("*").where (array_contains (comDefUpdated("Source_Target_Array"), 1))
    
    filterDF.show()
//
//    val metricDefColumns = Seq(
//      "MetricId",
//      "MetricSourceId",
//      "BatchId",
//      "MetricVersion",
//      "MetricValue").map(m => col(m))
//
//    val metricDataDF = spark.read
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/sparkdb")
//      .option("dbtable", "metric_data")
//      .option("user", "root")
//      .option("password", "root@123")
//      .load()
//      .withColumn("MetricSourceId", col("SourceId"))
//      .select(metricDefColumns: _*)
//
//    val metricMapComIdsSeq = metricComMapIdscsv.split(",").map(_.toString.toInt).toSeq
//
//    val filteredDf = metricComMapDF.filter(col("MetricComparisionMapId").isin(metricMapComIdsSeq: _*))
//
//    val metricBatchTrackerColumns = Seq(
//      "metricbatchtrackerid",
//      "BatchId",
//      "MetricComparisionMapId",
//      "MetricComparisionStatus").map(m => col(m))
//
//    val joinedMetricCompMapColumns = Seq(
//      "MetricComparisionMapId",
//      "ComparisionId",
//      "MetricId",
//      "BatchId",
//      "ThresholdPercentage").map(m => col(m))
//
//    val metricBatchTrackerAllDF = spark.read
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/sparkdb")
//      .option("dbtable", "metric_batch_tracker")
//      .option("user", "root")
//      .option("password", "root@123")
//      .load()
//      .select(metricBatchTrackerColumns: _*)
//
//    val metricBatchTrackerDF = metricBatchTrackerAllDF
//      .filter("MetricComparisionStatus='New'")
//      .filter(col("MetricComparisionMapId").isin(metricMapComIdsSeq: _*))
//
//    val metricCompMapDF = filteredDf.join(metricBatchTrackerDF, Seq("MetricComparisionMapId"))
//      .select(joinedMetricCompMapColumns: _*)
//      .distinct()
//
//    val joinedDf = metricCompMapDF.join(comDefinitionDF, Seq("ComparisionId"))
//
//    var reconLogMap: Map[String, Seq[Row]] = Map.empty[String, Seq[Row]]
//
//    var finalRowSeq: Seq[Row] = Seq.empty[Row]
//
//    joinedDf.collect.foreach(row => {
//      val metricCompMapId = row.getAs[Int]("MetricComparisionMapId")
//      val metricId = row.getAs[Int]("MetricId")
//      val batchId = row.getAs[String]("BatchId")
//      val sourceId = row.getAs[Int]("SourceId")
//      val targetId = row.getAs[Int]("TargetId")
//      val thresholdPer = row.getAs[Int]("ThresholdPercentage")
//
//      var recordSeq: Seq[Row] = Seq.empty[Row]
//
//      metricDataDF.collect.foreach(metricDataRow => {
//        val metricDataMetricId = metricDataRow.getAs[Int]("MetricId")
//        val metricDatasourceId = metricDataRow.getAs[Int]("MetricSourceId")
//        val metricBatchId = metricDataRow.getAs[String]("BatchId")
//        val metricVersion = metricDataRow.getAs[String]("MetricVersion")
//        val metricValue = metricDataRow.getAs[Int]("MetricValue")
//
//        if (metricId == metricDataMetricId && sourceId == metricDatasourceId && batchId.equals(metricBatchId)) {
//          val Array(reconLogSourceId, reconLogSourceValue) = Array(metricDatasourceId, metricValue)
//          val Array(reconLogTargetId, reconLogTargetValue) = Array(0, 0)
//          recordSeq = recordSeq :+ Row(metricBatchId, metricVersion, reconLogSourceId, reconLogSourceValue, reconLogTargetId, reconLogTargetValue, thresholdPer)
//        } else if (metricId == metricDataMetricId && targetId == metricDatasourceId && batchId.equals(metricBatchId)) {
//          val Array(reconLogTargetId, reconLogTargetValue) = Array(metricDatasourceId, metricValue)
//          val Array(reconLogSourceId, reconLogSourceValue) = Array(0, 0)
//          recordSeq = recordSeq :+ Row(metricBatchId, metricVersion, reconLogSourceId, reconLogSourceValue, reconLogTargetId, reconLogTargetValue, thresholdPer)
//        }
//      })
//      reconLogMap += (metricCompMapId + "@" + batchId -> recordSeq)
//    })
//
//    for ((key, rowSeq) <- reconLogMap) {
//
//      var MetricCompMapId = key.split("@")(0).toInt
//      var MetricBatch = key.split("@")(1)
//      var SourceId: Int = 0
//      var SourceValue: Int = 0
//      var TargetId: Int = 0
//      var TargetValue: Int = 0
//      var MetricVersion: String = ""
//      var ThresholdPercentage: Int = 0
//      var DeviationPercentage: Int = 0
//      var ReconStatus: String = ""
//
//      rowSeq.foreach(row => {
//        val sid = row(2).toString.toInt
//        val sval = row(3).toString.toInt
//        val tid = row(4).toString.toInt
//        val tval = row(5).toString.toInt
//
//        if (sid != 0)
//          SourceId = sid
//
//        if (sval != 0)
//          SourceValue = sval
//
//        if (tid != 0)
//          TargetId = tid
//
//        if (tval != 0)
//          TargetValue = tval
//
//        MetricVersion = row(1).toString
//        ThresholdPercentage = row(6).toString.toInt
//      })
//      
//      DeviationPercentage = (TargetValue - SourceValue).abs
//
//      if (DeviationPercentage <= ThresholdPercentage) {
//        ReconStatus = "Passed"
//      } else {
//        ReconStatus = "Failed"
//      }
//      finalRowSeq = finalRowSeq :+ Row(MetricCompMapId, MetricBatch, MetricVersion, SourceId, SourceValue, TargetId, TargetValue, ThresholdPercentage, DeviationPercentage, ReconStatus)
//    }
//
//    val finalRdd = spark.sparkContext.parallelize(finalRowSeq)
//    val finalDF = spark.createDataFrame(finalRdd, reconLogSchema)
//     .withColumn("Monotonic_Increasing_Id",monotonically_increasing_id())
//     .withColumn("ReconLogId",row_number().over(Window.orderBy(col("Monotonic_Increasing_Id"))))
//     .select(col("ReconLogId"),
//             col("MetricComparisionMapId"),
//             col("MetricBatch"),
//             col("MetricVersion"),
//             col("SourceId"),
//             col("SourceValue"),
//             col("TargetId"),
//             col("TargetValue"),
//             col("ThresholdPercentage"),
//             col("DeviationPercentage"),
//             col("ReconStatus"))
//
//    finalDF.write
//          .format("jdbc")
//          .mode(SaveMode.Overwrite)
//          .option("url", "jdbc:mysql://localhost:3306/sparkdb")
//          .option("dbtable", "reconciliation_log")
//          .option("user", "root")
//          .option("password", "root@123")
//          .save()
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("sparkreconframework").master("local").getOrCreate()
    insertReconMetricLog(spark, "1,2,3")
  }
}