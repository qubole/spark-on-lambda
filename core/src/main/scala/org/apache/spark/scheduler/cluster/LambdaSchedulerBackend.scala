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
      s"\t\tsparkDriverHostname=$sparkDriverHostname\n" +
      s"\t\tsparkDriverPort=$sparkDriverPort\n" +
      s"\t\tsparkCommandLine=$sparkCommandLine\n"
  }
}

case class LambdaRequestPayload (
  sparkS3Bucket: String,
  sparkS3Key: String,
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
  @LambdaFunction(functionName = "spark-lambda")
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

  val lambdaBucket = Option(sc.getConf.get("spark.lambda.s3.bucket"))

  if (!lambdaBucket.isDefined) {
    throw new Exception(s"spark.lambda.s3.bucket should" +
      s" have a valid S3 bucket name (eg: s3://lambda) having Spark binaries")
  }

  val lambdaFunctionName = sc.conf.get("spark.lambda.function.name", "spark-lambda")
  val s3SparkVersion = sc.conf.get("spark.lambda.spark.software.version", "LATEST")
  var numExecutorsExpected = 0
  var numExecutorsRegistered = new AtomicInteger(0)
  var executorId = new AtomicInteger(0)
  var numLambdaCallsPending = new AtomicInteger(0)
  // Mapping from executorId to Thread object which is currently in the Lambda RPC call
  var pendingLambdaRequests = new HashMap[String, Thread]
  // Set of executorIds which are currently alive
  val liveExecutors = new HashSet[String]

  var lambdaContainerMemory: Int = 0
  var lambdaContainerTimeoutSecs: Int = 0

  val clientConfig = new ClientConfiguration()
  clientConfig.setClientExecutionTimeout(345678)
  clientConfig.setConnectionTimeout(345679)
  clientConfig.setRequestTimeout(345680)
  clientConfig.setSocketTimeout(345681)

  val defaultClasspath = s"/tmp/lambda/spark/jars/*,/tmp/lambda/spark/conf/*"
  val lambdaClasspathStr = sc.conf.get("spark.lambda.classpath", defaultClasspath)
  val lambdaClasspath = lambdaClasspathStr.split(",").map(_.trim).mkString(":")

  val lambdaClient = AWSLambdaClientBuilder
                        .standard()
                        .withClientConfiguration(clientConfig)
                        .build()

  final val lambdaExecutorService: LambdaExecutorService =
    LambdaInvokerFactory.builder()
    .lambdaClient(lambdaClient)
    .build(classOf[LambdaExecutorService])
  logInfo(s"Created LambdaExecutorService: $lambdaExecutorService")

  val maxConcurrentRequests = sc.conf.getInt("spark.lambda.concurrent.requests.max", 100)
  val limiter = RateLimiter.create(maxConcurrentRequests)

  override def start() {
    super.start()
    logInfo("start")
    numExecutorsExpected = getInitialTargetExecutorNumber(conf)

    val request = new com.amazonaws.services.lambda.model.GetFunctionRequest
    request.setFunctionName(lambdaFunctionName)
    val result = lambdaClient.getFunction(request)
    logDebug(s"LAMBDA: 16000: Function details: ${result.toString}")

    val request2 = new com.amazonaws.services.lambda.model.GetFunctionConfigurationRequest
    request2.setFunctionName(lambdaFunctionName)
    val result2 = lambdaClient.getFunctionConfiguration(request2)
    lambdaContainerMemory = result2.getMemorySize
    lambdaContainerTimeoutSecs = result2.getTimeout
    logDebug(s"LAMBDA: 16001: Function configuration: ${result2.toString}")

    val request3 = new com.amazonaws.services.lambda.model.GetAccountSettingsRequest
    val result3 = lambdaClient.getAccountSettings(request3)
    logDebug(s"LAMBDA: 16002: Account settings: ${result3.toString}")
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
    logDebug(s"applicationId: $appId")
    return appId
  }

  private def launchExecutorsOnLambda(newExecutorsNeeded: Int) : Future[Boolean] = {
    Future {
      // TODO: Can we launch in parallel?
      // TODO: Can we track each thread separately and audit
      (1 to newExecutorsNeeded).foreach { x =>
        val hostname = sc.env.rpcEnv.address.host
        val port = sc.env.rpcEnv.address.port.toString
        val currentExecutorId = executorId.addAndGet(1)
        val containerId = applicationId() + "_%08d".format(currentExecutorId)

        val javaPartialCommandLine = s"java -cp ${lambdaClasspath} " +
            s"-server -Xmx${lambdaContainerMemory}m " +
            "-Djava.net.preferIPv4Stack=true " +
            s"-Dspark.driver.port=${port} " +
            "-Dspark.dynamicAllocation.enabled=true " +
            "-Dspark.shuffle.service.enabled=false "

        val executorPartialCommandLine = "org.apache.spark.executor.CoarseGrainedExecutorBackend " +
            s"--driver-url spark://CoarseGrainedScheduler@${hostname}:${port} " +
            s"--executor-id ${currentExecutorId} " +
            "--hostname LAMBDA " +
            "--cores 1 " +
            s"--app-id ${applicationId()} " +
            s"--user-class-path file:/tmp/lambda/* "

        val commandLine = javaPartialCommandLine + executorPartialCommandLine

        val request = new LambdaRequestPayload(
          sparkS3Bucket = lambdaBucket.get.split("/").last,
          sparkS3Key = s"lambda/spark-lambda-${s3SparkVersion}.zip",
          sparkDriverHostname = hostname,
          sparkDriverPort = port,
          sparkCommandLine = commandLine,
          javaPartialCommandLine = javaPartialCommandLine,
          executorPartialCommandLine = executorPartialCommandLine)

        case class LambdaRequesterThread(executorId: String, request: LambdaRequestPayload)
          extends Thread {
          override def run() {
            logDebug(s"LAMBDA: 9050: LambdaRequesterThread $executorId: $request")
            // Important code: Rate limit to avoid AWS errors
            limiter.acquire()
            logDebug(s"LAMBDA: 9050.1: LambdaRequesterThread started $executorId")
            numLambdaCallsPending.addAndGet(1)

            val invokeRequest = new InvokeRequest
            try {
              invokeRequest.setFunctionName(lambdaFunctionName)
              implicit val formats = Serialization.formats(NoTypeHints)
              val payload: String = Serialization.write(request)
              invokeRequest.setPayload(payload)
              logDebug(s"LAMBDA: 9050.2: request: ${payload}")
              val invokeResponse = lambdaClient.invoke(invokeRequest)
              logDebug(s"LAMBDA: 9051: Returned from lambda $executorId: $invokeResponse")
              val invokeResponsePayload: String =
                new String(invokeResponse.getPayload.array, java.nio.charset.StandardCharsets.UTF_8)
              logDebug(s"LAMBDA: 9051.1: Returned from lambda $executorId: $invokeResponsePayload")
            } catch {
              case t: Throwable => logError(s"Exception in Lambda invocation: $t")
            } finally {
              logDebug(s"LAMBDA: 9052: Returned from lambda $executorId: finally block")
              numLambdaCallsPending.addAndGet(-1)
              pendingLambdaRequests.remove(executorId)
              val responseMetadata = lambdaClient.getCachedResponseMetadata(invokeRequest)
              logDebug(s"LAMBDA: 9053: Response metadata: ${responseMetadata}")
            }
          }
        }

        val lambdaRequesterThread = LambdaRequesterThread(currentExecutorId.toString, request)
        pendingLambdaRequests(currentExecutorId.toString) = lambdaRequesterThread
        lambdaRequesterThread.setDaemon(true)
        lambdaRequesterThread.setName(s"Lambda Requester Thread for $currentExecutorId")
        logDebug(s"LAMBDA: 9055: starting lambda requester thread for $currentExecutorId")
        lambdaRequesterThread.start()
        logDebug(s"LAMBDA: 9056: returning from launchExecutorsOnLambda for $currentExecutorId")
      }
      true // TODO: Return true/false properly
    }
  }
  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    // TODO: Check again against numExecutorsExpected ??
    // We assume that all pending lambda calls are live lambdas and are fine
    val newExecutorsNeeded = requestedTotal - numLambdaCallsPending.get()
    logDebug(s"LAMBDA: 9001: doRequestTotalExecutors: ${newExecutorsNeeded} = " +
      s"${requestedTotal} - ${numLambdaCallsPending.get}")
    if (newExecutorsNeeded <= 0) {
      return Future { true }
    }
    return launchExecutorsOnLambda(newExecutorsNeeded)
  }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    // TODO: Right now not implemented
    logDebug(s"LAMBDA: 10200: doKillExecutors: $executorIds")
    Future {
      executorIds.foreach { x =>
        if (pendingLambdaRequests.contains(x)) {
          logDebug(s"LAMBDA: 10201: doKillExecutors: Interrupting $x")
          val thread = pendingLambdaRequests(x)
          pendingLambdaRequests.remove(x)
          thread.interrupt()
          logDebug(s"LAMBDA: 10202: ${thread.getState}")
        } else {
          logDebug(s"LAMBDA: 10203: doKillExecutor: $x is gone")
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
      logDebug(s"LAMBDA: 10100: onExecutorAdded: $event")
      logDebug(s"LAMBDA: 10100.1: onExecutorAdded: ${event.executorInfo.executorHost}")
      logDebug(s"LAMBDA: 10100.2: ${numExecutorsRegistered.get}")
      logDebug(s"LAMBDA: 10100.4: ${numLambdaCallsPending.get}")
      // TODO: synchronized block needed ??
      liveExecutors.add(event.executorId)
      numExecutorsRegistered.addAndGet(1)
    }
    override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
      logDebug(s"LAMBDA: 10101: onExecutorRemoved: $event")
      logDebug(s"LAMBDA: 10101.2: $numExecutorsRegistered")
      logDebug(s"LAMBDA: 10101.4: ${numLambdaCallsPending.get}")
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
      logDebug(s"LAMBDA: 10001: onDisconnected: $rpcAddress")
      super.onDisconnected(rpcAddress)
      logDebug("LAMBDA: 10002: onDisconnected")
    }
  }
  override def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    logDebug("LAMBDA: 10001: createDriverEndPoint: LambdaDriverEndpoint")
    new LambdaDriverEndpoint(rpcEnv, properties)
  }

  private val DEFAULT_NUMBER_EXECUTORS = 2
  private def getInitialTargetExecutorNumber(
                                      conf: SparkConf,
                                      numExecutors: Int = DEFAULT_NUMBER_EXECUTORS): Int = {
    if (Utils.isDynamicAllocationEnabled(conf)) {
      val minNumExecutors = conf.get(DYN_ALLOCATION_MIN_EXECUTORS)
      val initialNumExecutors =
        Utils.getDynamicAllocationInitialExecutors(conf)
      val maxNumExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS)
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
