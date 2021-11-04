package org.scaladatabaseutils

import org.scaladatabaseutils.dbutils.JdbcContextUtils

object ScalaDatabaseUtilsDemo {

  def main(args: Array[String]) {

    val path = "/home/bapu/Public/Workspaces/SupportWork/Spark/config/db-config.yaml"

    val jdbcContextUtils = JdbcContextUtils(path, "mysql", "sparkdb")

    val paramSeq = Seq(3, "Tata", "Tata Capital","Anand", "20211608", "Anand", "20211608")
    val insertQuery = "INSERT INTO source_definition (SourceId,SourceName,SourceDescription,CreatedBy,CreatedDt,UpdatedBy,UpdatedDt) VALUES (?, ?, ?, ?, ?, ?, ?)"

    jdbcContextUtils.executePrepStatement(insertQuery,paramSeq)
    
    val columnSeq = Seq("SourceId","SourceName","SourceDescription")
    val resultSet = jdbcContextUtils.selectTable("SourceId,SourceName,SourceDescription", "source_definition")

    while (resultSet.next()) {
      val sourceId = resultSet.getInt(1)
      val sourceName = resultSet.getString(2)
      val sourceDesc = resultSet.getString(3)
      println(sourceId + "#####" + sourceName + "#####" + sourceDesc)
    }
  }
}