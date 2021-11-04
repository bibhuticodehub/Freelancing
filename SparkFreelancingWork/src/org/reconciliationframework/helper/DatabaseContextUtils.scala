package org.reconciliationframework.helper


import org.apache.spark.sql.{SparkSession,DataFrame,SaveMode}

import java.util._
import java.io._
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import java.sql.DriverManager
import java.sql.Connection
import java.sql.Statement

object appConfig {

  private val prop = new Properties()
  prop.load(new FileInputStream("/home/bibhuti/Public/Workspaces/Spark/config/config.properties"))

  def config = prop
}

object DatabaseContextUtils {
  
  val JDBC_DRIVER = appConfig.config.getProperty("jdbc.driver")
  val JDBC_SCHEMA = appConfig.config.getProperty("jdbc.schema")
  val JDBC_URL = appConfig.config.getProperty("jdbc.url")
  val USERNAME = appConfig.config.getProperty("jdbc.username")
  val PASSWORD =appConfig.config.getProperty("jdbc.password")
}

class DatabaseContextUtils (val spark:SparkSession) {

    def tableWriter(df :DataFrame, tableName: String, saveMode:SaveMode): Unit = {
      try {
        df.write
          .format("jdbc")
          .mode(saveMode)
          .option("driver", DatabaseContextUtils.JDBC_DRIVER)
          .option("url", DatabaseContextUtils.JDBC_URL)
          .option("dbtable", tableName)
          .option("user", DatabaseContextUtils.USERNAME)
          .option("password", DatabaseContextUtils.PASSWORD)
          .save()
      } catch {
        case e : NoSuchTableException => throw new RuntimeException(s"Table $tableName not found",e)
      }
    }
    
    def saveTable(df :DataFrame, tableName: String): Unit = {
      tableWriter(df, tableName, SaveMode.Overwrite)
    }
    
    def appendToTable(df :DataFrame, tableName: String): Unit = {
      tableWriter(df, tableName, SaveMode.Append)
    }
    
    def insertToTableIfNotEXists(df :DataFrame, tableName: String): Unit = {
      val originalDF = table(tableName)
      val resultDF = originalDF.union(df).distinct()
      val tempTable = tableName+"_temp"
      saveTable(resultDF,tempTable)
      val tempTableDF = table(tempTable)
      saveTable(tempTableDF,tableName)
      dropTableIfExists(tempTable)
    }
    
    def insertToTableIfNotEXists(df :DataFrame, tableName: String, colNames: Seq[String]): Unit = {
      val originalDF = table(tableName)
      val resultDF = originalDF.union(df).dropDuplicates(colNames)
      val tempTable = tableName+"_temp"
      saveTable(resultDF,tempTable)
      val tempTableDF = table(tempTable)
      saveTable(tempTableDF,tableName)
      dropTableIfExists(tempTable)
    }
    
    def table(tableName:String): DataFrame = {
      try {
        spark.read
          .format("jdbc")
          .option("driver", DatabaseContextUtils.JDBC_DRIVER)
          .option("url", DatabaseContextUtils.JDBC_URL)
          .option("dbtable", tableName)
          .option("user", DatabaseContextUtils.USERNAME)
          .option("password", DatabaseContextUtils.PASSWORD)
          .load()
      } catch {
        case e : NoSuchTableException => throw new RuntimeException(s"Table $tableName not found",e)
      }
    }
    
    def dropTableIfExists(tableName:String): Unit = {
      var conn: Connection = null;
      var stmt: Statement = null;
    
      try {
        Class.forName(DatabaseContextUtils.JDBC_DRIVER)
        conn = DriverManager.getConnection(DatabaseContextUtils.JDBC_URL, DatabaseContextUtils.USERNAME, DatabaseContextUtils.PASSWORD);
        stmt = conn.createStatement();
        val sql: String = s"DROP TABLE IF EXISTS ${tableName} ";
        stmt.executeUpdate(sql);
      } catch {
        case e : NoSuchTableException => throw new RuntimeException(s"Table $tableName not found",e)
      } finally {
        stmt.close()
        conn.close()
      }
    }
}

