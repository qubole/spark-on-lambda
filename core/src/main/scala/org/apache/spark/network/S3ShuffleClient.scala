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

package org.apache.spark.network

import org.apache.spark.internal.Logging
import org.apache.spark.network.shuffle.{BlockFetchingListener, RetryingBlockFetcher, ShuffleClient}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.storage.BlockManager

class S3ShuffleClient(transportConf: TransportConf,
                      blockManager: BlockManager) extends ShuffleClient with Logging {
  override def fetchBlocks(
               host: String,
               port: Int,
               execId: String,
               blockIds: Array[String],
               listener: BlockFetchingListener): Unit = {
    logTrace(s"Fetch blocks from $host:$port (executor id $execId)")
    try {
      val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
        override def createAndStart(blockIds: Array[String],
                                    listener: BlockFetchingListener): Unit = {
          new S3BlockFetcher(blockManager, execId, blockIds, listener).start
        }
      }

      val maxRetries = transportConf.maxIORetries()
      if (maxRetries > 0) {
        // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
        // a bug in this code. We should remove the if statement once we're sure of the stability.
        new RetryingBlockFetcher(transportConf, blockFetchStarter, blockIds, listener).start()
      } else {
        blockFetchStarter.createAndStart(blockIds, listener)
      }
    } catch {
      case e: Exception =>
        log.error("Exception while beginning fetchBlocks", e)
        for (blockId <- blockIds) {
          listener.onBlockFetchFailure(blockId, e)
        }
    }
  }

  override def close(): Unit = { }
}
