package org.freelancing.reconciliationframework

import java.sql.Connection
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.Map


object AtomicCounter {  
  private val atomicCounter = new AtomicInteger()
  def nextCount(): Int = atomicCounter.getAndIncrement()
}

case class InputMetricData(SourceName: String, MetricName: String)

case class SOURCEDEFINITION(sourceId: Int, sourceName: String, sourceDescription: String)
case class METRICDEFINITION(metricId: Int, metricName: String, metricDescription: String)
case class COMPARISIONDEFINITION(comparisionId	: Int, sourceId: Int, targetId: Int)
case class METRICDATA(metricId	: Int, sourceId: Int, batchId: String, metricVersion: String, metricValue:Int)

case class RECONLOG(
  MetricComparisionMapId: Int,
  MetricBatch:            String,
  MetricVersion:          String,
  SourceId:               Int,
  SourceValue:            Int,
  TargetId:               Int,
  TargetValue:            Int,
  ThresholdPercentage:    Int,
  DeviationPercentage:    Int,
  ReconStatus:            String)

class BaseReconMetricAddition private (val connection: Connection, val dbName: String) {

}

object BaseReconMetricAddition {

  private var CONNECTION: Connection = null
  var inputMetricData: InputMetricData = null
  lazy val SOURCE_MAP = getSourceMap
  lazy val METRIC_MAP = getMetricMap
  lazy val COMPARISION_DEF_MAP = getCompDefMap
  lazy val METRIC_DATA_MAP = getMetricDataMap

  def getSourceMap(): Map[String, SOURCEDEFINITION] = {

    var sourceMap = Map.empty[String, SOURCEDEFINITION]

    val statement = BaseReconMetricAddition.CONNECTION.createStatement
    val sourceResultSet = statement.executeQuery("select sourceId, sourceName, sourceDescription from sparkdb.source_definition")
    while (sourceResultSet.next()) {
      val sourceId = sourceResultSet.getInt(1)
      val sourceName = sourceResultSet.getString(2)
      val sourceDescription = sourceResultSet.getString(3)
      sourceMap += (sourceName -> SOURCEDEFINITION(sourceId, sourceName, sourceDescription))
    }
    sourceMap
  }

  def getMetricMap(): Map[String, METRICDEFINITION] = {

    var metricMap = Map.empty[String, METRICDEFINITION]
    val statement = BaseReconMetricAddition.CONNECTION.createStatement
    val metricResultSet = statement.executeQuery("select metricId, metricName, metricDescription from sparkdb.metric_definition")
    while (metricResultSet.next()) {
      val metricId = metricResultSet.getInt(1)
      val metricName = metricResultSet.getString(2)
      val metricDescription = metricResultSet.getString(3)
      metricMap += (metricName -> METRICDEFINITION(metricId, metricName, metricDescription))
    }
    metricMap
  }

  def getCompDefMap(): Map[Int, COMPARISIONDEFINITION] = {

    var comparisionMap = Map.empty[Int, COMPARISIONDEFINITION]
    val statement = BaseReconMetricAddition.CONNECTION.createStatement
    val comparisionResult = statement.executeQuery("select ComparisionId, SourceId, TargetId from sparkdb.comparision_definition")
    while (comparisionResult.next()) {
      val comparisionId = comparisionResult.getInt(1)
      val sourceId = comparisionResult.getInt(2)
      val targetId = comparisionResult.getInt(3)
      comparisionMap += (comparisionId -> COMPARISIONDEFINITION(comparisionId, sourceId, targetId))
    }
    comparisionMap
  }
  
  def getMetricDataMap(): Map[String, METRICDATA] = {
    
    var metricDataMap = Map.empty[String, METRICDATA]
    val statement = BaseReconMetricAddition.CONNECTION.createStatement
    val metricDataResultSet = statement.executeQuery("select MetricId, SourceId, BatchId, MetricVersion, MetricValue from sparkdb.metric_data")
    while (metricDataResultSet.next()) {
      val metricId = metricDataResultSet.getInt(1)
      val sourceId = metricDataResultSet.getInt(2)
      val batchId = metricDataResultSet.getString(3)
      val metricVersion = metricDataResultSet.getString(4)
      val metricValue = metricDataResultSet.getInt(5)
      metricDataMap += (metricVersion -> METRICDATA(metricId, sourceId, batchId, metricVersion, metricValue))
    }
    metricDataMap
  }
  
  def apply(dbConnection: Connection) = {
    CONNECTION = dbConnection
    this.inputMetricData = inputMetricData
  }

  def getConnectionHashCode(): Int = {
    BaseReconMetricAddition.CONNECTION.hashCode
  }

  def getDataMapHashCode(): String = {
    "SOURCEMAP_HASHCODE=" + BaseReconMetricAddition.SOURCE_MAP.hashCode + " and METRICMAP_HASHCODE=" + BaseReconMetricAddition.METRIC_MAP.hashCode

  }

  def getSourceDetails(sourceName: String): Int = {
    SOURCE_MAP(sourceName).sourceId
  }

  def getMetricDetails(metricName: String): Int = {
    METRIC_MAP(metricName).metricId
  }

  def insertReconMetricData(): Unit = {
    val souceDescription = getSourceDetails(inputMetricData.SourceName)
    val metricDescription = getMetricDetails(inputMetricData.MetricName)

    println(souceDescription + "##" + metricDescription)
  }

  def insertReconLogData(mapComparisionMapIdCsv: String): Unit = {
      
    var commaSeparatedVal = mapComparisionMapIdCsv.split(",").mkString("','")
    commaSeparatedVal = "'" + commaSeparatedVal + "'"

    val sql = "SELECT * FROM sparkdb.metric_comparision_map where MetricComparisionMapId in (" + commaSeparatedVal + ")"
    val statement = CONNECTION.createStatement
    val resultset = statement.executeQuery(sql)

    val reconLogMap: Map[Int, Seq[Map[String, Any]]] = Map.empty[Int, Seq[Map[String, Any]]]
    val finalReconLogMap: Map[String, Any] = Map.empty[String, Any]

    val comparisionsql = "SELECT * FROM sparkdb.comparision_definition where ComparisionId = ?"
    val comparisionPrepStatement = CONNECTION.prepareStatement(comparisionsql)
    
    val metricdatasql = "SELECT * FROM sparkdb.metric_data where MetricId = ? and SourceId in (?,?)"
    val metricDataPrepStatement = CONNECTION.prepareStatement(metricdatasql)
    
    while (resultset.next()) {
      val metricComparisionId = resultset.getInt(1)
      val metricId = resultset.getInt(2)
      val comparisionId = resultset.getInt(3)
      val thresholdPer = resultset.getInt(4)

      comparisionPrepStatement.setInt(1,comparisionId)
      val compResultset = comparisionPrepStatement.executeQuery()

      var recordSeq: Seq[Map[String, Any]] = Seq.empty[Map[String, Any]]

      while (compResultset.next()) {
        val sourceId = compResultset.getInt(2)
        val targetId = compResultset.getInt(3)

        metricDataPrepStatement.setInt(1,metricId)
        metricDataPrepStatement.setInt(2,sourceId)
        metricDataPrepStatement.setInt(3,targetId)
        val metricResultset = metricDataPrepStatement.executeQuery()

        while (metricResultset.next()) {
          val metricDataMetricId = metricResultset.getInt(1)
          val metricDatasourceId = metricResultset.getInt(2)
          val metricBatchId = metricResultset.getString(3)
          val metricVersion = metricResultset.getString(4)
          val metricValue = metricResultset.getInt(5)

          val Array(reconLogSourceId, reconLogSourceValue) = if (sourceId == metricDatasourceId) Array(metricDatasourceId, metricValue) else Array(0, 0)
          val Array(reconLogTargetId, reconLogTargetValue) = if (targetId == metricDatasourceId) Array(metricDatasourceId, metricValue) else Array(0, 0)

          recordSeq = recordSeq :+ Map(
            "MetricBatch" -> metricBatchId,
            "MetricVersion" -> metricVersion,
            "SourceId" -> reconLogSourceId,
            "SourceValue" -> reconLogSourceValue,
            "TargetId" -> reconLogTargetId,
            "TargetValue" -> reconLogTargetValue,
            "ThresholdPercentage" -> thresholdPer)
        }
        reconLogMap += (metricComparisionId -> recordSeq)
      }
    }
    
    val reconInsertSql = s"""INSERT INTO sparkdb.reconciliation_log 
                                    (ReconLogId,
                                      MetricComparisionMapId,
                                      MetricBatch,
                                      MetricVersion,
                                      SourceId,
                                      SourceValue,
                                      TargetId,
                                      TargetValue,
                                      ThresholdPercentage,
                                      DeviationPercentage,
                                      ReconStatus)
                                      VALUES
                                      (?,
                                       ?,
                                       ?,
                                       ?,
                                       ?,
                                       ?,
                                       ?,
                                       ?,
                                       ?,
                                       ?,
                                       ?)"""
    
    
    val reconInsertPrepStatement = CONNECTION.prepareStatement(reconInsertSql)
    
    for ((metricComparisionId, valueSeq) <- reconLogMap) {

      var SourceId: Int = 0
      var SourceValue: Int = 0
      var TargetId: Int = 0
      var TargetValue: Int = 0
      var MetricBatch: String = ""
      var MetricVersion: String = ""
      var ThresholdPercentage: Int = 0
      var DeviationPercentage: Int = 0
      var ReconStatus: String = ""

      if (valueSeq.size > 0) {
        // Loop through the sequence and try to create a single map with all the log details
        valueSeq.foreach(valueMap => {
          if (valueMap("SourceId") != 0)
            SourceId = valueMap("SourceId").toString.toInt

          if (valueMap("SourceValue") != 0)
            SourceValue = valueMap("SourceValue").toString.toInt

          if (valueMap("TargetId") != 0)
            TargetId = valueMap("TargetId").toString.toInt

          if (valueMap("TargetValue") != 0)
            TargetValue = valueMap("TargetValue").toString.toInt

          MetricBatch = valueMap("MetricBatch").toString
          MetricVersion = valueMap("MetricVersion").toString
          ThresholdPercentage = valueMap("ThresholdPercentage").toString.toInt
        })

        DeviationPercentage = (TargetValue - SourceValue).abs

        if (DeviationPercentage <= ThresholdPercentage) {
          ReconStatus = "Passed"
        } else {
          ReconStatus = "Failed"
        }

        val counter = AtomicCounter.nextCount+1
        
        reconInsertPrepStatement.setInt(1, counter)
        reconInsertPrepStatement.setInt(2, metricComparisionId)
        reconInsertPrepStatement.setString(3, MetricBatch)
        reconInsertPrepStatement.setString(4, MetricVersion)
        reconInsertPrepStatement.setInt(5, SourceId)
        reconInsertPrepStatement.setInt(6, SourceValue)
        reconInsertPrepStatement.setInt(7, TargetId)
        reconInsertPrepStatement.setInt(8, TargetValue)
        reconInsertPrepStatement.setInt(9, ThresholdPercentage)
        reconInsertPrepStatement.setInt(10, DeviationPercentage)
        reconInsertPrepStatement.setString(11, ReconStatus)
        
        reconInsertPrepStatement.addBatch
      }
    }
    reconInsertPrepStatement.executeBatch
    reconInsertPrepStatement.close
  }
  
  def insertReconLogCacheData(mapComparisionMapIdCsv: String): Unit = {
      
    var commaSeparatedVal = mapComparisionMapIdCsv.split(",").mkString("','")
    commaSeparatedVal = "'" + commaSeparatedVal + "'"

    val sql = "SELECT * FROM sparkdb.metric_comparision_map where MetricComparisionMapId in (" + commaSeparatedVal + ")"
    val statement = CONNECTION.createStatement
    val resultset = statement.executeQuery(sql)

    val reconLogMap: Map[Int, Seq[Map[String, Any]]] = Map.empty[Int, Seq[Map[String, Any]]]
    val finalReconLogMap: Map[String, Any] = Map.empty[String, Any]

    val comparisionsql = "SELECT * FROM sparkdb.comparision_definition where ComparisionId = ?"
    val comparisionPrepStatement = CONNECTION.prepareStatement(comparisionsql)
    
    val metricdatasql = "SELECT * FROM sparkdb.metric_data where MetricId = ? and SourceId in (?,?)"
    val metricDataPrepStatement = CONNECTION.prepareStatement(metricdatasql)
    
    while (resultset.next()) {
      val metricComparisionId = resultset.getInt(1)
      val metricId = resultset.getInt(2)
      val comparisionId = resultset.getInt(3)
      val thresholdPer = resultset.getInt(4)

      comparisionPrepStatement.setInt(1,comparisionId)
      val compResultset = comparisionPrepStatement.executeQuery()

      var recordSeq: Seq[Map[String, Any]] = Seq.empty[Map[String, Any]]

      while (compResultset.next()) {
        val sourceId = compResultset.getInt(2)
        val targetId = compResultset.getInt(3)

        metricDataPrepStatement.setInt(1,metricId)
        metricDataPrepStatement.setInt(2,sourceId)
        metricDataPrepStatement.setInt(3,targetId)
        val metricResultset = metricDataPrepStatement.executeQuery()

        while (metricResultset.next()) {
          val metricDataMetricId = metricResultset.getInt(1)
          val metricDatasourceId = metricResultset.getInt(2)
          val metricBatchId = metricResultset.getString(3)
          val metricVersion = metricResultset.getString(4)
          val metricValue = metricResultset.getInt(5)

          val Array(reconLogSourceId, reconLogSourceValue) = if (sourceId == metricDatasourceId) Array(metricDatasourceId, metricValue) else Array(0, 0)
          val Array(reconLogTargetId, reconLogTargetValue) = if (targetId == metricDatasourceId) Array(metricDatasourceId, metricValue) else Array(0, 0)

          recordSeq = recordSeq :+ Map(
            "MetricBatch" -> metricBatchId,
            "MetricVersion" -> metricVersion,
            "SourceId" -> reconLogSourceId,
            "SourceValue" -> reconLogSourceValue,
            "TargetId" -> reconLogTargetId,
            "TargetValue" -> reconLogTargetValue,
            "ThresholdPercentage" -> thresholdPer)
        }
        reconLogMap += (metricComparisionId -> recordSeq)
      }
    }
    
    val reconInsertSql = s"""INSERT INTO sparkdb.reconciliation_log 
                                    (ReconLogId,
                                      MetricComparisionMapId,
                                      MetricBatch,
                                      MetricVersion,
                                      SourceId,
                                      SourceValue,
                                      TargetId,
                                      TargetValue,
                                      ThresholdPercentage,
                                      DeviationPercentage,
                                      ReconStatus)
                                      VALUES
                                      (?,
                                       ?,
                                       ?,
                                       ?,
                                       ?,
                                       ?,
                                       ?,
                                       ?,
                                       ?,
                                       ?,
                                       ?)"""
    
    
    val reconInsertPrepStatement = CONNECTION.prepareStatement(reconInsertSql)
    
    for ((metricComparisionId, valueSeq) <- reconLogMap) {

      var SourceId: Int = 0
      var SourceValue: Int = 0
      var TargetId: Int = 0
      var TargetValue: Int = 0
      var MetricBatch: String = ""
      var MetricVersion: String = ""
      var ThresholdPercentage: Int = 0
      var DeviationPercentage: Int = 0
      var ReconStatus: String = ""

      if (valueSeq.size > 0) {
        // Loop through the sequence and try to create a single map with all the log details
        valueSeq.foreach(valueMap => {
          if (valueMap("SourceId") != 0)
            SourceId = valueMap("SourceId").toString.toInt

          if (valueMap("SourceValue") != 0)
            SourceValue = valueMap("SourceValue").toString.toInt

          if (valueMap("TargetId") != 0)
            TargetId = valueMap("TargetId").toString.toInt

          if (valueMap("TargetValue") != 0)
            TargetValue = valueMap("TargetValue").toString.toInt

          MetricBatch = valueMap("MetricBatch").toString
          MetricVersion = valueMap("MetricVersion").toString
          ThresholdPercentage = valueMap("ThresholdPercentage").toString.toInt
        })

        DeviationPercentage = (TargetValue - SourceValue).abs

        if (DeviationPercentage <= ThresholdPercentage) {
          ReconStatus = "Passed"
        } else {
          ReconStatus = "Failed"
        }

        val counter = AtomicCounter.nextCount+1
        
        reconInsertPrepStatement.setInt(1, counter)
        reconInsertPrepStatement.setInt(2, metricComparisionId)
        reconInsertPrepStatement.setString(3, MetricBatch)
        reconInsertPrepStatement.setString(4, MetricVersion)
        reconInsertPrepStatement.setInt(5, SourceId)
        reconInsertPrepStatement.setInt(6, SourceValue)
        reconInsertPrepStatement.setInt(7, TargetId)
        reconInsertPrepStatement.setInt(8, TargetValue)
        reconInsertPrepStatement.setInt(9, ThresholdPercentage)
        reconInsertPrepStatement.setInt(10, DeviationPercentage)
        reconInsertPrepStatement.setString(11, ReconStatus)
        
        reconInsertPrepStatement.addBatch
      }
    }
    reconInsertPrepStatement.executeBatch
    reconInsertPrepStatement.close
  }

  def doBulkInsert(inputMetricDataList: List[InputMetricData]): Unit = {
    var bulkInsertSql = "INSERT INTO metric_data (metricDataId,MetricId,SourceId,BatchId,MetricVersion,MetricValue,CreatedTimeSTamp,CreatedBy) VALUES "
    var metricDataCounterId = 1

    inputMetricDataList.foreach(inputMetricDataObj => {
      val sourceId = getSourceDetails(inputMetricDataObj.SourceName)
      val metricId = getMetricDetails(inputMetricDataObj.MetricName)
      bulkInsertSql = bulkInsertSql + "(" + metricDataCounterId + "," + metricId + "," + sourceId + ",'20211028','V2',50,'20211028','Anand'),"
      metricDataCounterId = metricDataCounterId + 1
    })
    bulkInsertSql = bulkInsertSql.stripSuffix(",").trim
    val sqlPrepStatement = CONNECTION.prepareStatement(bulkInsertSql)
    sqlPrepStatement.execute()
  }
}