package org.freelancing.reconciliationframework;

object ReconciliationDriver {

  def main(args: Array[String]) {

    val path = "/home/bapu/Public/Workspaces/Freelancing/config/db-config.yaml"

    val jdbcUtils = JdbcContextUtils(path, "mysql", "sparkdb")
    val connection = jdbcUtils.getConnection

    val inputMetricObj1 = InputMetricData("Guidewire", "Policy")
    val inputMetricObj2 = InputMetricData("Aviva", "Claims")
    val inputMetricObj3 = InputMetricData("Tata", "Billing")

    val inputMetricDataList = List(inputMetricObj1, inputMetricObj2, inputMetricObj3)

    BaseReconMetricAddition(connection)
    BaseReconMetricAddition.doBulkInsert(inputMetricDataList)

    jdbcUtils.closeConnection
  }
}