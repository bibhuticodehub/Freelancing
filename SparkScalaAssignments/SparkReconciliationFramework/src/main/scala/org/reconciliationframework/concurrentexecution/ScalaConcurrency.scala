package org.reconciliationframework.concurrentexecution

import java.util.concurrent.Executors

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutorService
import scala.concurrent.Future
import scala.concurrent.duration.Duration

object ScalaConcurrency {

  def doParallelInsertMetricData(inputParam: String)(implicit ec: ExecutionContextExecutorService): Future[Unit] = Future {
    println(s"Starting task for $inputParam")
    Thread.sleep(3000) // wait 2secs
    println(s"Finished task for $inputParam")
  }

  def main(args: Array[String]) {
     
    println("Starting Main")
    
    val availableProcessor = Runtime.getRuntime.availableProcessors

    val paramSeq = Seq("Param1", "Param2", "Param3", "Param4", "Param5", "Param6", "Param7", "Param8")

    implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(availableProcessor))
    
    val tasks = Future.traverse(paramSeq)(x => doParallelInsertMetricData(x)(ec))
    val completedFutureResultFuture = Await.result(tasks, Duration.Inf)
    println("Finishing Main")
    ec.shutdownNow
  }
}