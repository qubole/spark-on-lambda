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
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.storage.BlockManager

class S3BlockFetcher(blockManager: BlockManager,
                     execId: String,
                     blockIds: Array[String],
                     listener: BlockFetchingListener) extends Logging {
  def start : Unit = {
    if (blockIds.length == 0) throw new IllegalArgumentException("Zero-sized blockIds array")

    try {
      for (blockId <- blockIds) {
        logDebug(s"${blockIds.mkString(", ")}")
        listener.onBlockFetchSuccess(blockId, blockManager.getRemoteBlockData(blockId, execId))
      }
    } catch {
      case ex: Exception => failRemainingBlocks(blockIds, ex)
    }
  }

  /** Invokes the "onBlockFetchFailure" callback for every listed block id. */
  private def failRemainingBlocks(failedBlockIds: Array[String], e: Throwable) = {
    for (blockId <- failedBlockIds) {
      try
        listener.onBlockFetchFailure(blockId, e)
      catch {
        case e2: Exception =>
          logError("Error in block fetch failure callback", e2)
      }
    }
  }
}
