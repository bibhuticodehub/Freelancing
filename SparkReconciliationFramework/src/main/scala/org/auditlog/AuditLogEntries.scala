package org.auditlog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

object AuditLogEntries {

  def nvl(ColIdNew: Column, colIdOld: Column): Column = {
    return (when(ColIdNew.isNull, colIdOld).otherwise(ColIdNew))
  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("auditlog").master("local").getOrCreate()

    val auditLogRawDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sparkdb")
      .option("dbtable", "audit_log")
      .option("user", "root")
      .option("password", "root@123")
      .load()

    val partitionedColumns = Seq("id", "op_code_pri")
    val byIdAndOpCodeOpTimeDesc = Window.partitionBy(col("id"), col("op_code_pri")).orderBy(col("op_time").desc)
    val rowNumberClause = row_number().over(byIdAndOpCodeOpTimeDesc)
    val orderByClause = Window.orderBy(col("id").asc, col("op_code_pri").asc, col("op_time").asc)

    val mappltDF = auditLogRawDF
      .withColumn("id", nvl(col("id_new"), col("id_old")))
      .withColumn("op_code_pri", when(col("Op_Code") === "I", lit(1))
        .when(col("Op_Code") === "U", lit(2))
        .otherwise(lit(3)))
      .select("id", "Op_Code", "op_code_pri", "op_time")
      .withColumn("row_num", rowNumberClause)
      .orderBy(col("id").asc, col("op_code_pri").asc, col("op_time").asc)
      .where(col("row_num") === 1)
      .orderBy(col("id").asc, col("op_code_pri").asc, col("op_time").asc)
      .withColumn("next_id", lead(col("id"), 1).over(orderByClause))
      .withColumn("next_op_code", lead(col("op_code"), 1).over(orderByClause))
      .withColumn("next_op_time", lead(col("op_time"), 1).over(orderByClause))
      .withColumn("prev_id", lag(col("id"), 1).over(orderByClause))
      .withColumn("prev_op_code", lag(col("op_code"), 1).over(orderByClause))
      .withColumn("prev_op_time", lag(col("op_time"), 1).over(orderByClause))
      .withColumn("prev_to_prev_id", lag(col("id"), 2).over(orderByClause))
      .withColumn("prev_to_prev_op_code", lag(col("op_code"), 2).over(orderByClause))
      .withColumn("prev_to_prev_op_time", lag(col("op_time"), 2).over(orderByClause))
      .withColumn("Flag", when(
        col("id") === col("next_id")
          && col("op_time") === col("next_op_time")
          && col("op_code") === "I"
          && col("next_op_code") === "U",
        lit("No"))
        .when(
          col("id") === col("next_id")
            && col("op_time") === col("next_op_time")
            && (col("op_code") === "I" || col("op_code") === "U")
            && col("next_op_code") === "D",
          lit("No"))
        .when(
          col("id") === col("prev_id")
            && col("op_time") === col("prev_op_time")
            && col("prev_op_code") === "I"
            && col("op_code") === "D",
          lit("No"))
        .when(
          col("id") === col("prev_id")
            && col("id") === col("prev_to_prev_id")
            && col("op_time") === col("prev_op_time")
            && col("op_time") === col("prev_to_prev_op_time")
            && col("op_code") === "D"
            && col("prev_op_code") === "U"
            && col("prev_to_prev_op_code") === "I",
          lit("No"))
        .otherwise(lit("Yes")))
      .select(
        col("id"),
        col("op_time"),
        col("op_code"),
        col("Flag"))

    val resultDF = auditLogRawDF.alias("m1").join(mappltDF, (auditLogRawDF("id_new") === mappltDF("id") || auditLogRawDF("id_old") === mappltDF("id"))
      && (auditLogRawDF("op_time") === mappltDF("op_time"))
      && (auditLogRawDF("op_code") === mappltDF("op_code")), "inner").filter(col("Flag") === "Yes")
      .select("m1.*")

    resultDF.write
          .format("jdbc")
          .mode(SaveMode.Append)
          .option("url", "jdbc:mysql://localhost:3306/sparkdb")
          .option("dbtable", "output_auditlog")
          .option("user", "root")
          .option("password", "root@123")
          .save()
  }
}