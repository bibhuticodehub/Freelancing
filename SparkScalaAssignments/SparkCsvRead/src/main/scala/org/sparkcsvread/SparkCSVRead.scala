package org.sparkcsvread

import org.apache.spark.sql.SparkSession

object SparkCSVRead {

  
  def getJobMetaDataInsertQueries(spark:SparkSession, metdataTblName: String, batchTblName: String): Seq[String] = {
    
    var finalInsertSqlSeq = Seq.empty[String] 
    
    val csvDf = spark.read.format("csv").option("header", "true").load("/home/bibhuti/Public/Workspaces/Spark/config/etlaudit_jobmetadata.csv")
    
    val columns = csvDf.columns
    val columnHeader = columns.mkString(",")
    
    val selectStatementsArr = csvDf.collect.foreach(row=>{
      val jobId:String = row.getAs[String]("jobid")
      var insertHiveQl = "INSERT INTO  TABLE  staging_gwpolicycenter_cldpoc."+metdataTblName
      var dummyRowHiveQl = "INSERT INTO TABLE staging_gwpolicycenter_cldpoc."+batchTblName
      insertHiveQl = insertHiveQl+"("+columnHeader+")"
      val rowStr = "'"+row.mkString("\',\'")
      val selectExpr = "SELECT "+rowStr.patch(rowStr.lastIndexOf("'"), "", 1)
      val dummyRowSelectExpr = "SELECT '20210917"+jobId+"',CURRENT_TIMESTAMP(),'Completed','"+jobId+"',CURRENT_TIMESTAMP()"
      insertHiveQl = insertHiveQl+" "+selectExpr +";"
      dummyRowHiveQl = dummyRowHiveQl+ " "+ dummyRowSelectExpr+";"
      
      finalInsertSqlSeq = finalInsertSqlSeq:+insertHiveQl
      finalInsertSqlSeq = finalInsertSqlSeq:+dummyRowHiveQl
    })
    
    finalInsertSqlSeq
  }
  
  def getTaskMetaDataInsertQueries(spark:SparkSession, taskMetdataTblName: String, savePointTblName: String): Seq[String] = {
    
    var finalInsertSqlSeq = Seq.empty[String] 
    
    val csvDf = spark.read.format("csv").option("header", "true").load("/home/bibhuti/Public/Workspaces/Spark/config/etlaudit_taskmetadata.csv")
    
    val columns = csvDf.columns
    val columnHeader = columns.mkString(",")
    
    val selectStatementsArr = csvDf.collect.foreach(row=>{
      val jobId:String = row.getAs[String]("jobid")
      var insertHiveQl = "INSERT INTO  TABLE  staging_gwpolicycenter_cldpoc."+taskMetdataTblName
      var dummyRowHiveQl = "INSERT INTO TABLE staging_gwpolicycenter_cldpoc."+savePointTblName
      insertHiveQl = insertHiveQl+"("+columnHeader+")"
      val rowStr = "'"+row.mkString("\',\'")
      val selectExpr = "SELECT "+rowStr.patch(rowStr.lastIndexOf("'"), "", 1)
      val dummyRowSelectExpr = "SELECT '20210917"+jobId+"','"+jobId+"','SAMTSK1',CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP(),null,null,'Success',0,'True',CURRENT_TIMESTAMP()"
      insertHiveQl = insertHiveQl+" "+selectExpr +";"
      dummyRowHiveQl = dummyRowHiveQl+ " "+ dummyRowSelectExpr+";"
      
      finalInsertSqlSeq = finalInsertSqlSeq:+insertHiveQl
      finalInsertSqlSeq = finalInsertSqlSeq:+dummyRowHiveQl
    })
    
    finalInsertSqlSeq
  }
  
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("sparkcopycsvread").master("local").getOrCreate()
    
    val jobMetadataInsertQueries = getJobMetaDataInsertQueries(spark, "etlaudit_jobmetadata","etlaudit_jbatch")
    
    for(jobMetaDataSql <- jobMetadataInsertQueries) {
      println(jobMetaDataSql)
    }
    
    val taskMetadataInsertQueries = getTaskMetaDataInsertQueries(spark, "etlaudit_taskmetadata","etlauditsavepoint")
    
    for(taskMetaDataSql <- taskMetadataInsertQueries) {
      println(taskMetaDataSql)
    }
  }
}