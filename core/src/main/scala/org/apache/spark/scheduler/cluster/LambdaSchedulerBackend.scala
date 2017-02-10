/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster

import java.util.concurrent.{Executors, ThreadPoolExecutor}
import java.util.concurrent.atomic.AtomicInteger

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.lambda.AWSLambdaClientBuilder
import com.amazonaws.services.lambda.invoke.{LambdaFunction, LambdaInvokerFactory}
import com.amazonaws.services.lambda.model.InvokeRequest
import com.google.common.util.concurrent.RateLimiter
import org.json4s._
import org.json4s.jackson.Serialization

import scala.collection.mutable.{HashMap, HashSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.apache.spark.SparkContext
import org.apache.spark.internal.config._
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.scheduler._
import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

class Request {
  var sparkS3Bucket: String = _
  def getSparkS3Bucket: String = sparkS3Bucket
  def setSparkS3Bucket(i: String): Unit = { sparkS3Bucket = i }

  var sparkS3Key: String = _
  def getSparkS3Key: String = sparkS3Key
  def setSparkS3Key(i: String): Unit = { sparkS3Key = i }

  var hadoop2S3Bucket: String = _
  def getHadoop2S3Bucket: String = hadoop2S3Bucket
  def setHadoop2S3Bucket(i: String): Unit = { hadoop2S3Bucket = i }

  var hadoop2S3Key: String = _
  def getHadoop2S3Key: String = hadoop2S3Key
  def setHadoop2S3Key(i: String): Unit = { hadoop2S3Key = i }

  var hive12S3Bucket: String = _
  def getHive12S3Bucket: String = hive12S3Bucket
  def setHive12S3Bucket(i: String): Unit = { hive12S3Bucket = i }

  var hive12S3Key: String = _
  def getHive12S3Key: String = hive12S3Key
  def setHive12S3Key(i: String): Unit = { hive12S3Key = i }

  var sparkDriverHostname: String = _
  def getSparkDriverHostname: String = sparkDriverHostname
  def setSparkDriverHostname(i: String): Unit = { sparkDriverHostname = i }

  var sparkDriverPort: String = _
  def getSparkDriverPort: String = sparkDriverPort
  def setSparkDriverPort(i: String): Unit = { sparkDriverPort = i }

  var sparkCommandLine: String = _
  def getSparkCommandLine: String = sparkCommandLine
  def setSparkCommandLine(i: String): Unit = { sparkCommandLine = i }

  override def toString() : String = {
    s"Lambda Request: sparkS3Bucket=$sparkS3Bucket\n" +
      s"\t\tsparkS3Key=$sparkS3Key\n" +
      s"\t\thadoop2S3Bucket=$hadoop2S3Bucket\n" +
      s"\t\thadoop2S3Key=$hadoop2S3Key\n" +
      s"\t\thive12S3Bucket=$hive12S3Bucket\n" +
      s"\t\thive12S3Key=$hive12S3Key\n" +
      s"\t\tsparkDriverHostname=$sparkDriverHostname\n" +
      s"\t\tsparkDriverPort=$sparkDriverPort\n" +
      s"\t\tsparkCommandLine=$sparkCommandLine\n"
  }
}

case class LambdaRequestPayload (
  sparkS3Bucket: String,
  sparkS3Key: String,
  hadoop2S3Bucket: String,
  hadoop2S3Key: String,
  hive12S3Bucket: String,
  hive12S3Key: String,
  sparkDriverHostname: String,
  sparkDriverPort: String,
  sparkCommandLine: String,
  javaPartialCommandLine: String,
  executorPartialCommandLine: String
)

class Response {
  var output: String = _
  def getOutput() : String = output
  def setOutput(o: String) : Unit = { output = o }
  override def toString() : String = {
    s"Lambda Response: output=$output"
  }
}

trait LambdaExecutorService {
  @LambdaFunction(functionName = "get_spark_from_s3_unusable_now_across_vpcs")
  def runExecutor(request: Request) : Response
}

/**
 * A [[SchedulerBackend]] implementation for Spark's AWS Lambda cluster manager.
 */
private[spark] class LambdaSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    masters: Array[String])
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv)
  with Logging {

  val lambdaFunctionName = sc.conf.get("spark.qubole.lambda.function.name", "get_spark_from_s3")
  val s3SparkVersion = sc.conf.get("spark.qubole.lambda.spark.software.version", "LATEST")
  var numExecutorsExpected = 0
  var numExecutorsRegistered = new AtomicInteger(0)
  var executorId = new AtomicInteger(0)
  var numLambdaCallsPending = new AtomicInteger(0)
  // Mapping from executorId to Thread object which is currently in the Lambda RPC call
  var pendingLambdaRequests = new HashMap[String, Thread]
  // Set of executorIds which are currently alive
  val liveExecutors = new HashSet[String]

  var lambdaContainerMemoryBytes: Int = 0
  var lambdaContainerTimeoutSecs: Int = 0

  val clientConfig = new ClientConfiguration()
  clientConfig.setClientExecutionTimeout(345678)
  clientConfig.setConnectionTimeout(345679)
  clientConfig.setRequestTimeout(345680)
  clientConfig.setSocketTimeout(345681)

  final val lambdaExecutorService: LambdaExecutorService =
    LambdaInvokerFactory.builder()
    .lambdaClient(AWSLambdaClientBuilder.standard().withClientConfiguration(clientConfig).build())
    .build(classOf[LambdaExecutorService])
  logInfo(s"Created LambdaExecutorService: $lambdaExecutorService")

  val maxConcurrentRequests = sc.conf.getInt("spark.qubole.lambda.concurrent.requests.max", 100)
  val limiter = RateLimiter.create(maxConcurrentRequests)

  override def start() {
    super.start()
    logInfo("start")
    numExecutorsExpected = getInitialTargetExecutorNumber(conf)

    val lambdaClient = AWSLambdaClientBuilder.defaultClient()

    val request = new com.amazonaws.services.lambda.model.GetFunctionRequest
    request.setFunctionName(lambdaFunctionName)
    val result = lambdaClient.getFunction(request)
    logInfo(s"LAMBDA: 16000: Function details: ${result.toString}")

    val request2 = new com.amazonaws.services.lambda.model.GetFunctionConfigurationRequest
    request2.setFunctionName(lambdaFunctionName)
    val result2 = lambdaClient.getFunctionConfiguration(request2)
    lambdaContainerMemoryBytes = result2.getMemorySize * 1024 * 1024
    lambdaContainerTimeoutSecs = result2.getTimeout
    logInfo(s"LAMBDA: 16001: Function configuration: ${result2.toString}")

    val request3 = new com.amazonaws.services.lambda.model.GetAccountSettingsRequest
    val result3 = lambdaClient.getAccountSettings(request3)
    logInfo(s"LAMBDA: 16002: Account settings: ${result3.toString}")
  }

  override def stop(): Unit = synchronized {
    logInfo("stop")
  }

  override def sufficientResourcesRegistered(): Boolean = {
    val ret = totalRegisteredExecutors.get() >= numExecutorsExpected * minRegisteredRatio
    logInfo(s"sufficientResourcesRegistered: $ret ${totalRegisteredExecutors.get()}")
    ret
  }

  override def applicationId(): String = {
    val appId = super.applicationId()
    logInfo(s"applicationId: $appId")
    return appId
  }

  private def launchExecutorsOnLambda(newExecutorsNeeded: Int) : Future[Boolean] = {
    Future {
      // TODO: Can we launch in parallel?
      // TODO: Can we track each thread separately and audit
      (1 to newExecutorsNeeded).foreach { x =>
        val request = new Request
        request.setSparkS3Bucket("bharatb")
        request.setSparkS3Key(s"lambda/spark-small-${s3SparkVersion}.zip")
        request.setHadoop2S3Bucket("bharatb")
        request.setHadoop2S3Key(s"lambda/hadoop2-small-${s3SparkVersion}.zip")
        request.setHive12S3Bucket("bharatb")
        request.setHive12S3Key(s"lambda/hive1.2-small-${s3SparkVersion}.zip")
        val hostname = sc.env.rpcEnv.address.host
        val port = sc.env.rpcEnv.address.port.toString
        request.setSparkDriverHostname(hostname)
        request.setSparkDriverPort(port)

        val classpathSeq = Seq("spark/assembly/target/scala-2.11/jars/*",
          "spark/conf",
          "hadoop2/share/hadoop/*",
          "hadoop2/share/hadoop/common/lib/*",
          "hadoop2/share/hadoop/common/*",
          "hadoop2/share/hadoop/hdfs",
          "hadoop2/share/hadoop/hdfs/lib/*",
          "hadoop2/share/hadoop/hdfs/*",
          "hadoop2/share/hadoop/yarn/lib/*",
          "hadoop2/share/hadoop/yarn/*",
          "hadoop2/share/hadoop/mapreduce/*",
          "hadoop2/share/hadoop/tools/lib/*",
          "hadoop2/share/hadoop/tools/*",
          "hadoop2/share/hadoop/qubole/lib/*",
          "hadoop2/share/hadoop/qubole/*",
          "hadoop2/etc/hadoop/*",
          "hive1.2/lib/*"
        )
        val classpaths = classpathSeq.map(x => s"/tmp/lambda/$x").mkString(":")
        val currentExecutorId = executorId.addAndGet(1)
        val containerId = applicationId() + "_%08d".format(currentExecutorId)
        request.setSparkCommandLine(
          s"java -cp ${classpaths} " +
            "-server -Xmx1400m " +
            "-Djava.net.preferIPv4Stack=true " +
            s"-Dspark.driver.port=${port} " +
            // "-Dspark.blockManager.port=12345 " +
            "-Dspark.dynamicAllocation.enabled=true " +
            "-Dspark.shuffle.service.enabled=false " +
            "org.apache.spark.executor.CoarseGrainedExecutorBackend " +
            s"--driver-url spark://CoarseGrainedScheduler@${hostname}:${port} " +
            s"--executor-id ${currentExecutorId} " +
            "--hostname LAMBDA " +
            "--cores 1 " +
            s"--app-id ${applicationId()} " +
            s"--container-id ${containerId} " +
            s"--container-size ${lambdaContainerMemoryBytes} " +
            "--user-class-path file:/tmp/lambda/* "
        )

        val lambdaRequesterThread = new Thread() {
          override def run() {
            val executorId = currentExecutorId.toString
            logInfo(s"LAMBDA: 9002: Invoking lambda for $executorId: $request")
            numLambdaCallsPending.addAndGet(1)
            try {
              val response = lambdaExecutorService.runExecutor(request)
              logInfo(s"LAMBDA: 9003: Returned from lambda $executorId: $response")
            } catch {
              case t: Throwable => logError(s"Exception in Lambda invocation: $t")
            } finally {
              logInfo(s"LAMBDA: 9003: Returned from lambda $executorId: finally block")
              numLambdaCallsPending.addAndGet(-1)
              pendingLambdaRequests.remove(executorId)
            }
          }
        }
        lambdaRequesterThread.setDaemon(true)
        lambdaRequesterThread.setName(s"Lambda Requester Thread for $currentExecutorId")
        pendingLambdaRequests(currentExecutorId.toString) = lambdaRequesterThread
        logInfo(s"LAMBDA: 9004: starting lambda requester thread for $currentExecutorId")
        lambdaRequesterThread.start()

        logInfo(s"LAMBDA: 9005: returning from launchExecutorsOnLambda for $currentExecutorId")
      }
      true // TODO: Return true/false properly
    }
  }

  private def launchExecutorsOnLambda2(newExecutorsNeeded: Int) : Future[Boolean] = {
    Future {
      // TODO: Can we launch in parallel?
      // TODO: Can we track each thread separately and audit
      (1 to newExecutorsNeeded).foreach { x =>
        val hostname = sc.env.rpcEnv.address.host
        val port = sc.env.rpcEnv.address.port.toString
        val classpathSeq = Seq("spark/assembly/target/scala-2.11/jars/*",
          "spark/conf",
          "hadoop2/share/hadoop/*",
          "hadoop2/share/hadoop/common/lib/*",
          "hadoop2/share/hadoop/common/*",
          "hadoop2/share/hadoop/hdfs",
          "hadoop2/share/hadoop/hdfs/lib/*",
          "hadoop2/share/hadoop/hdfs/*",
          "hadoop2/share/hadoop/yarn/lib/*",
          "hadoop2/share/hadoop/yarn/*",
          "hadoop2/share/hadoop/mapreduce/*",
          "hadoop2/share/hadoop/tools/lib/*",
          "hadoop2/share/hadoop/tools/*",
          "hadoop2/share/hadoop/qubole/lib/*",
          "hadoop2/share/hadoop/qubole/*",
          "hadoop2/etc/hadoop/*",
          "hive1.2/lib/*"
        )
        val classpaths = classpathSeq.map(x => s"/tmp/lambda/$x").mkString(":")
        val currentExecutorId = executorId.addAndGet(1)
        val containerId = applicationId() + "_%08d".format(currentExecutorId)

        val javaPartialCommandLine = s"java -cp ${classpaths} " +
            "-server -Xmx1400m " +
            "-Djava.net.preferIPv4Stack=true " +
            s"-Dspark.driver.port=${port} " +
            // "-Dspark.blockManager.port=12345 " +
            "-Dspark.dynamicAllocation.enabled=true " +
            "-Dspark.shuffle.service.enabled=false "
        val executorPartialCommandLine = "org.apache.spark.executor.CoarseGrainedExecutorBackend " +
            s"--driver-url spark://CoarseGrainedScheduler@${hostname}:${port} " +
            s"--executor-id ${currentExecutorId} " +
            "--hostname LAMBDA " +
            "--cores 1 " +
            s"--app-id ${applicationId()} " +
            s"--container-id ${containerId} " +
            s"--container-size ${lambdaContainerMemoryBytes} " +
            "--user-class-path file:/tmp/lambda/* "
        val commandLine = javaPartialCommandLine + executorPartialCommandLine

        val request = new LambdaRequestPayload(
          sparkS3Bucket = "bharatb",
          sparkS3Key = s"lambda/spark-small-${s3SparkVersion}.zip",
          hadoop2S3Bucket = "bharatb",
          hadoop2S3Key = s"lambda/hadoop2-small-${s3SparkVersion}.zip",
          hive12S3Bucket = "bharatb",
          hive12S3Key = s"lambda/hive1.2-small-${s3SparkVersion}.zip",
          sparkDriverHostname = hostname,
          sparkDriverPort = port,
          sparkCommandLine = commandLine,
          javaPartialCommandLine = javaPartialCommandLine,
          executorPartialCommandLine = executorPartialCommandLine)

        case class LambdaRequesterThread(executorId: String, request: LambdaRequestPayload)
          extends Thread {
          override def run() {
            logInfo(s"LAMBDA: 9050: LambdaRequesterThread $executorId: $request")
            // Important code: Rate limit to avoid AWS errors
            limiter.acquire()
            logInfo(s"LAMBDA: 9050.1: LambdaRequesterThread started $executorId")
            numLambdaCallsPending.addAndGet(1)
            // TODO: Can we reuse the same client across calls?
            val lambdaClient = AWSLambdaClientBuilder.standard()
              .withClientConfiguration(clientConfig).build()
            val invokeRequest = new InvokeRequest
            try {
              invokeRequest.setFunctionName(lambdaFunctionName)
              implicit val formats = Serialization.formats(NoTypeHints)
              val payload: String = Serialization.write(request)
              invokeRequest.setPayload(payload)
              logInfo(s"LAMBDA: 9050.2: request: ${payload}")
              val invokeResponse = lambdaClient.invoke(invokeRequest)
              logInfo(s"LAMBDA: 9051: Returned from lambda $executorId: $invokeResponse")
              val invokeResponsePayload: String =
                new String(invokeResponse.getPayload.array, java.nio.charset.StandardCharsets.UTF_8)
              logInfo(s"LAMBDA: 9051.1: Returned from lambda $executorId: $invokeResponsePayload")
            } catch {
              case t: Throwable => logError(s"Exception in Lambda invocation: $t")
            } finally {
              logInfo(s"LAMBDA: 9052: Returned from lambda $executorId: finally block")
              numLambdaCallsPending.addAndGet(-1)
              pendingLambdaRequests.remove(executorId)
              val responseMetadata = lambdaClient.getCachedResponseMetadata(invokeRequest)
              logInfo(s"LAMBDA: 9053: Response metadata: ${responseMetadata}")
            }
          }
        }

        val lambdaRequesterThread = LambdaRequesterThread(currentExecutorId.toString, request)
        pendingLambdaRequests(currentExecutorId.toString) = lambdaRequesterThread
        lambdaRequesterThread.setDaemon(true)
        lambdaRequesterThread.setName(s"Lambda Requester Thread for $currentExecutorId")
        logInfo(s"LAMBDA: 9055: starting lambda requester thread for $currentExecutorId")
        lambdaRequesterThread.start()
        logInfo(s"LAMBDA: 9056: returning from launchExecutorsOnLambda for $currentExecutorId")
      }
      true // TODO: Return true/false properly
    }
  }
  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    // TODO: Check again against numExecutorsExpected ??
    // We assume that all pending lambda calls are live lambdas and are fine
    val newExecutorsNeeded = requestedTotal - numLambdaCallsPending.get()
    logInfo(s"LAMBDA: 9001: doRequestTotalExecutors: ${newExecutorsNeeded} = " +
      s"${requestedTotal} - ${numLambdaCallsPending.get}")
    if (newExecutorsNeeded <= 0) {
      return Future { true }
    }
    return launchExecutorsOnLambda2(newExecutorsNeeded)
  }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    // TODO: Fill this function
    logInfo(s"LAMBDA: 10200: doKillExecutors: $executorIds")
    Future {
      executorIds.foreach { x =>
        if (pendingLambdaRequests.contains(x)) {
          logInfo(s"LAMBDA: 10201: doKillExecutors: Interrupting $x")
          val thread = pendingLambdaRequests(x)
          pendingLambdaRequests.remove(x)
          thread.interrupt()
          logInfo(s"LAMBDA: 10202: ${thread.getState}")
        } else {
          logInfo(s"LAMBDA: 10203: doKillExecutor: $x is gone")
        }
      }
      true
    }
  }

  private val lambdaSchedulerListener = new LambdaSchedulerListener(scheduler.sc.listenerBus)
  private class LambdaSchedulerListener(listenerBus: LiveListenerBus)
    extends SparkListener with Logging {

    listenerBus.addListener(this)

    override def onExecutorAdded(event: SparkListenerExecutorAdded) {
      logInfo(s"LAMBDA: 10100: onExecutorAdded: $event")
      logInfo(s"LAMBDA: 10100.1: onExecutorAdded: ${event.executorInfo.executorHost}")
      logInfo(s"LAMBDA: 10100.2: ${numExecutorsRegistered.get}")
      logInfo(s"LAMBDA: 10100.4: ${numLambdaCallsPending.get}")
      // TODO: synchronized block needed ??
      liveExecutors.add(event.executorId)
      numExecutorsRegistered.addAndGet(1)
    }
    override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
      logInfo(s"LAMBDA: 10101: onExecutorRemoved: $event")
      logInfo(s"LAMBDA: 10101.2: $numExecutorsRegistered")
      logInfo(s"LAMBDA: 10101.4: ${numLambdaCallsPending.get}")
      liveExecutors.remove(event.executorId)
      numExecutorsRegistered.addAndGet(-1)
    }
  }

  /**
    * Override the DriverEndpoint to add extra logic for the case when an executor is disconnected.
    * This endpoint communicates with the executors and queries the AM for an executor's exit
    * status when the executor is disconnected.
    */
  private class LambdaDriverEndpoint(rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])
    extends DriverEndpoint(rpcEnv, sparkProperties) {

    // TODO Fix comment below
    /**
      * When onDisconnected is received at the driver endpoint, the superclass DriverEndpoint
      * handles it by assuming the Executor was lost for a bad reason and removes the executor
      * immediately.
      *
      * In YARN's case however it is crucial to talk to the application master and ask why the
      * executor had exited. If the executor exited for some reason unrelated to the running tasks
      * (e.g., preemption), according to the application master, then we pass that information down
      * to the TaskSetManager to inform the TaskSetManager that tasks on that lost executor should
      * not count towards a job failure.
      */
    override def onDisconnected(rpcAddress: RpcAddress): Unit = {
      logInfo(s"LAMBDA: 10001: onDisconnected: $rpcAddress")
      super.onDisconnected(rpcAddress)
      logInfo("LAMBDA: 10002: onDisconnected")
    }
  }
  override def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    logInfo("LAMBDA: 10001: createDriverEndPoint: LambdaDriverEndpoint")
    new LambdaDriverEndpoint(rpcEnv, properties)
  }

  private val DEFAULT_NUMBER_EXECUTORS = 2
  private def getInitialTargetExecutorNumber(
                                      conf: SparkConf,
                                      numExecutors: Int = DEFAULT_NUMBER_EXECUTORS): Int = {
    if (Utils.isDynamicAllocationEnabled(conf)) {
      val minNumExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", 0)
      val initialNumExecutors =
        Utils.getDynamicAllocationInitialExecutors(conf)
      val maxNumExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors", Int.MaxValue)
      require(initialNumExecutors >= minNumExecutors && initialNumExecutors <= maxNumExecutors,
        s"initial executor number $initialNumExecutors must between min executor number " +
          s"$minNumExecutors and max executor number $maxNumExecutors")

      initialNumExecutors
    } else {
      val targetNumExecutors =
        sys.env.get("SPARK_EXECUTOR_INSTANCES").map(_.toInt).getOrElse(numExecutors)
      // System property can override environment variable.
      conf.get(EXECUTOR_INSTANCES).getOrElse(targetNumExecutors)
    }
  }
}

private[spark] object LambdaSchedulerBackend {
  val ENDPOINT_NAME = "LambdaScheduler"
}
