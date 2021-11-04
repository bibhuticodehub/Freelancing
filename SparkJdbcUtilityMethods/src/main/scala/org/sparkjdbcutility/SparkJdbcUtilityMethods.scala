package org.sparkjdbcutility

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.scaladatabaseutils.dbutils.JdbcContextUtils
import org.apache.hadoop.fs.FileSystem

object SparkJdbcUtilityMethods {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("sparkcopyfromlocal").master("local").enableHiveSupport().getOrCreate()

    val path = "/home/bibhuti/Public/Workspaces/Spark/config/db-config.yaml"
//    val parquetFilePath = "/home/bibhuti/Public/Workspaces/Spark/config/employee"

    val jdbcutils = JdbcContextUtils(path, "mysql", "sparkdb")

    val jdbcUrl = jdbcutils.getJdbcUrl()
    val dbName = jdbcutils.getDbName()

//    val jdbcDF = spark.read
//      .format("jdbc")
//      .option("url", jdbcUrl)
//      .option("dbtable", "sparkdb.employee")
//      .option("user", "root")
//      .option("password", "root@123")
//      .load()

//     val jdbcDF = spark.table(dbName+"."+"employee")
     
//     val jdbcDF = spark.sql("select * from "+dbName+".employee")
    
     val tableName = "employee"
     val jdbcDF = spark.sql("select * from "+dbName+"."+tableName)
     val parquetFilePath = "/home/bibhuti/Public/Workspaces/Spark/config/"+tableName
     
    jdbcDF.coalesce(1).write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save(parquetFilePath)

    // Copy from local to HDFS path
    val conf = new Configuration
    conf.addResource(new Path("/usr/local/hadoop/hadoop-3.3.1/etc/hadoop/core-site.xml"))
    val hdfs = FileSystem.get(conf)

    val srcPath = new org.apache.hadoop.fs.Path("file://"+parquetFilePath)
    val destPath = new org.apache.hadoop.fs.Path("hdfs:///usr/data/employee/")

    if (hdfs.exists(destPath)) {
      hdfs.delete(destPath, true)
    }

    hdfs.copyFromLocalFile(srcPath, destPath)
  }
}

