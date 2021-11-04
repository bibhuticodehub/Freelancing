package org.reconciliationframework.helper

object ParamConstants {
  val SOURCE_NAME = "SourceName"
  val METRIC_NAME = "MetricName"
  val METRIC_VALUE = "MetricValue"
  val BATCH_ID = "BatchId"
  val USER = "User"
}

object ReconciliationMetricsHelper {
  
  def getParsedParameters(args:Array[String]):Map[String,String] = {
    
    var paramsMap:Map[String, String] = Map[String,String]()
    
    args.foreach(elem => {
        val paramValue = elem.split(":")(1)
        
        if(elem.contains(ParamConstants.SOURCE_NAME)) {
            paramsMap += (ParamConstants.SOURCE_NAME -> paramValue)
        } else if(elem.contains(ParamConstants.METRIC_NAME)) {
              paramsMap += (ParamConstants.METRIC_NAME -> paramValue)
        } else if(elem.contains(ParamConstants.METRIC_VALUE)) {
              paramsMap += (ParamConstants.METRIC_VALUE -> paramValue)
        } else if(elem.contains(ParamConstants.BATCH_ID)) {
              paramsMap += (ParamConstants.BATCH_ID -> paramValue)
        } else if(elem.contains(ParamConstants.USER)){
              paramsMap += (ParamConstants.USER -> paramValue)
        }
    })
    paramsMap
  }
}