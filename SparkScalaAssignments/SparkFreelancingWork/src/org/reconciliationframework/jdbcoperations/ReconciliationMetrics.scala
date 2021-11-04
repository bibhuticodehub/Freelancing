package org.reconciliationframework.jdbcoperations

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

import org.reconciliationframework.helper.ParamConstants._
import org.reconciliationframework.helper.ReconciliationMetricsHelper._
import org.reconciliationframework.helper.DatabaseContextUtils
import org.reconciliationframework.tabledefinitions.MetricData

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}

import java.util.Properties

object ReconciliationMetrics {

  def main(args: Array[String]) {

    val args = Array("SourceName:General","MetricName:Policy","MetricValue:200","BatchId:20210925","User:Anand")
    
    val argsMap = getParsedParameters(args)
    
    val sourceName = argsMap(SOURCE_NAME)
    val metricName = argsMap(METRIC_NAME)
    val metricValue = argsMap(METRIC_VALUE)
    val batchId = argsMap(BATCH_ID)
    val user = argsMap(USER)
    
    val spark = SparkSession.builder().master("local").getOrCreate()
    
    val dbContextUtils:DatabaseContextUtils = new DatabaseContextUtils(spark)
    
    val metriDataDF = dbContextUtils.table("metric_data")
    
    val sourceDefDF = dbContextUtils.table("source_definition").where(col("SourceName")===sourceName)
    val metricDF = dbContextUtils.table("metric_definition").where(col("MetricName")===metricName)
    
    val sourceId = sourceDefDF.select("SourceId").first().get(0).toString.toInt
    val metricId = metricDF.select("MetricId").first().get(0).toString.toInt
    
    val filteredData = Seq((metricId, sourceId, batchId, 12, metricValue, "20210918", user))
    val filteredDF = spark.createDataFrame(filteredData)
                               .toDF(MetricData.METRIC_ID, MetricData.SOURCE_ID,
                                     MetricData.BATCH_ID,MetricData.METRIC_VERSION,
                                     MetricData.METRIC_VALUE,MetricData.CREATED_TIME_STAMP,MetricData.CREATED_BY)
  
    val colNames = Seq(MetricData.SOURCE_ID,MetricData.METRIC_ID)
    
    dbContextUtils.insertToTableIfNotEXists(filteredDF,"metric_data",colNames)
     
  }
}