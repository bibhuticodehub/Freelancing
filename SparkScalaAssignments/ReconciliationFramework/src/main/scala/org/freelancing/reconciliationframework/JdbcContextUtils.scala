package org.freelancing.reconciliationframework

import java.sql.DriverManager
import java.sql.Connection

import org.freelancing.reconciliationframework.yaml.YamlReaderUtils._
import scala.collection.JavaConverters._
import java.sql.ResultSet

case class DbPropertiesConfig(
  platform: String,
  driver:   String,
  url:      String,
  database: String,
  username: String,
  password: String)

object DbPropertiesConfig {

  def fromMap(map: Map[String, String]): DbPropertiesConfig = {
    DbPropertiesConfig(
      platform = map("platform"),
      driver = map("driver"),
      url = map("url"),
      database = map("database"),
      username = map("username"),
      password = map("password"))
  }
}

class JdbcContextUtils private (private val connection: Connection, private val jdbcUrl: String, private val dbName: String) {

  def getConnection(): Connection = connection

  def getJdbcUrl(): String = jdbcUrl

  def getDbName(): String = dbName

  def selectTable(selectExpr: String, tableName: String): ResultSet = {
    val qualifiedTableName = dbName + "." + tableName
    val sqlQuery = s"SELECT ${selectExpr} FROM ${qualifiedTableName}"
    val sqlPrepStatement = connection.prepareStatement(sqlQuery)
    sqlPrepStatement.executeQuery()
  }

  def selectTable(columnNames: Seq[String], tableName: String): ResultSet = {
    val qualifiedTableName = dbName + "." + tableName
    val columnExpr = columnNames.mkString(",")
    val sqlQuery = s"SELECT ${columnExpr} FROM ${qualifiedTableName}"
    val sqlPrepStatement = connection.prepareStatement(sqlQuery)
    sqlPrepStatement.executeQuery()
  }
  
  def selectTable(selectExpr: String, tableName: String, whereCond: String): ResultSet = {
    val qualifiedTableName = dbName + "." + tableName
    val sqlQuery = s"SELECT ${selectExpr} FROM ${qualifiedTableName} WHERE ${whereCond}"
    val sqlPrepStatement = connection.prepareStatement(sqlQuery)
    sqlPrepStatement.executeQuery()
  }
  
  def selectTable(columnNames: Seq[String], tableName: String, whereCond: String): ResultSet = {
    val qualifiedTableName = dbName + "." + tableName
    val columnExpr = columnNames.mkString(",")
    val whereCondExpr = whereCond.mkString(" AND ")
    val sqlQuery = s"SELECT ${columnExpr} FROM ${qualifiedTableName} WHERE ${whereCondExpr}"
    val sqlPrepStatement = connection.prepareStatement(sqlQuery)
    sqlPrepStatement.executeQuery()
  }
  
  def selectTable(selectExpr: String, tableName: String, whereCond: Seq[String]): ResultSet = {
    val qualifiedTableName = dbName + "." + tableName
    val sqlQuery = s"SELECT ${selectExpr} FROM ${qualifiedTableName} WHERE ${whereCond}"
    val sqlPrepStatement = connection.prepareStatement(sqlQuery)
    sqlPrepStatement.executeQuery()
  }
  
  def selectTable(columnNames: Seq[String], tableName: String, whereCond: Seq[String]): ResultSet = {
    val qualifiedTableName = dbName + "." + tableName
    val columnExpr = columnNames.mkString(",")
    val whereCondExpr = whereCond.mkString(" AND ")
    val sqlQuery = s"SELECT ${columnExpr} FROM ${qualifiedTableName} WHERE ${whereCondExpr}"
    val sqlPrepStatement = connection.prepareStatement(sqlQuery)
    sqlPrepStatement.executeQuery()
  }

  def execute(sqlQuery: String): Unit = {
    val sqlPrepStatement = connection.prepareStatement(sqlQuery)
    sqlPrepStatement.execute()
  }

  def executePrepStatement(sqlQuery: String, params: Seq[Any]): Unit = {
    
    val sqlPrepStatement = connection.prepareStatement(sqlQuery)
    
    for (i <- 0 until params.length) {
       val prepareStatementParamNum = i+1
       val paramType = params(i).getClass.getName
       
       paramType match {
        case "java.lang.String" => sqlPrepStatement.setString(prepareStatementParamNum, params(i).toString)
        case "java.lang.Integer" => sqlPrepStatement.setInt(prepareStatementParamNum, params(i).toString.toInt)
        case "java.lang.Double" => sqlPrepStatement.setDouble(prepareStatementParamNum, params(i).toString.toDouble)
        case _ => "other"
      }
    }    
    sqlPrepStatement.execute()
  }
  
  def closeConnection(): Unit = {
    connection.close()
  }
}

object JdbcContextUtils {

  def apply(yamlPath: String, platform: String, dbName: String): JdbcContextUtils = {

    try {
      val yamlString = readConfigFile(yamlPath)
      val dbPropertiesMap = convertYamlToMap(yamlString)

      val propertiesMap = dbPropertiesMap.get("datasource")
        .asInstanceOf[java.util.ArrayList[java.util.HashMap[String, String]]]
        .asScala
        .toSeq

      var dbEntryMap = Map.empty[String, String]

      for (propMap <- propertiesMap) {
        if (propMap.get("platform").equals(platform) && propMap.get("database").equals(dbName)) {
          dbEntryMap = propMap.asScala.toMap
        }
      }

      val dbConfig = DbPropertiesConfig.fromMap(dbEntryMap)

      //      val dbConfig = DbPropertiesConfig.fromMap(dbPropertiesMap.get("datasource")
      //        .asInstanceOf[java.util.ArrayList[java.util.HashMap[String, String]]]
      //        .stream
      //        .filter(dbentry => {
      //          dbentry.get("platform").equals(platform) && dbentry.get("database").equals(dbName)
      //        })
      //        .findFirst.get
      //        .asScala
      //        .toMap)
      //
      val driver = dbConfig.driver
      val dbUrl = dbConfig.url + dbConfig.database

      /* The Keystore logic should be implemented here */
      val username = dbConfig.username
      val password = dbConfig.password

      Class.forName(driver)
      val connection = DriverManager.getConnection(dbUrl, username, password)
      new JdbcContextUtils(connection, dbConfig.url, dbConfig.database)
    } catch {
      case e: Throwable =>
        throw new Exception("Error in Database connection", e)
    }
  }
}