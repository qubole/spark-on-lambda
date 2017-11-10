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

package org.apache.spark.shuffle

import java.io._

import com.google.common.io.ByteStreams
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{ManagedBuffer, S3SegmentManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, ShuffleDataBlockId, ShuffleIndexBlockId}
import org.apache.spark.util.Utils

 /*
  * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
  * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
  * The offsets of the data blocks in the data file are stored in a separate index file.
  * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
  * as the filename postfix for data file, and ".index" as the filename postfix for index file.
  */
private[spark] class S3ShuffleBlockResolver(
    conf: SparkConf,
    _blockManager: BlockManager = null)
    extends IndexShuffleBlockResolver(conf, _blockManager = null)
    with Logging {
  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)

  val shuffleS3Bucket = BlockManager.getS3Bucket(conf)

  private lazy val hadoopConf = BlockManager.getHadoopConf(conf)

  private lazy val hadoopFileSystem = BlockManager.getHadoopFileSystem(conf)

  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")

  override def getDataFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /**
    * Remove data file and index file that contain the output data from one map.
    * */
  override def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting data ${file.getPath()}")
      }
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting index ${file.getPath()}")
      }
    }
  }

  /**
    * Check whether the given index and data files match each other.
    * If so, return the partition lengths in the data file. Otherwise return null.
    */
  private def checkIndexAndDataFile(index: File, data: File, blocks: Int): Array[Long] = {
    val indexFilePath = Utils.localFileToS3(shuffleS3Bucket, index)
    try {
      if (hadoopFileSystem.getFileStatus(indexFilePath).getLen != (blocks + 1) * 8) {
        return null
      }
    } catch {
      case fnf: FileNotFoundException => return null
      case io: IOException => return null
    }

    val lengths = new Array[Long](blocks)
    // Read the lengths of blocks
    val in =
      try {
        hadoopFileSystem.open(indexFilePath)
      } catch {
        case e: IOException =>
          return null
      }

    try {
      // Convert the offsets into lengths of each block
      var offset = in.readLong()
      if (offset != 0L) {
        return null
      }
      var i = 0
      while (i < blocks) {
        val off = in.readLong()
        lengths(i) = off - offset
        offset = off
        i += 1
      }
    } catch {
      case e: IOException =>
        return null
    } finally {
      in.close()
    }

    val dataPath = Utils.localFileToS3(shuffleS3Bucket, data)

    if (hadoopFileSystem.getFileStatus(dataPath).getLen == lengths.sum) {
      logInfo(s"${dataPath} lengths (${lengths.sum}) match with index file length")
      lengths
    } else {
      logInfo(s"${dataPath} lengths (${lengths.sum} didn't match with index file length")
      null
    }
  }

  /**
    * Write an index file with the offsets of each block, plus a final offset at the end for the
    * end of the output file. This will be used by getBlockData to figure out where each block
    * begins and ends.
    *
    * It will commit the data and index file as an atomic operation, use the existing ones, or
    * replace them with new ones.
    *
    * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
    */
  override def writeIndexFileAndCommit(
               shuffleId: Int,
               mapId: Int,
               lengths: Array[Long],
               dataTmp: File): Unit = {
    val indexFile = getIndexFile(shuffleId, mapId)
    val indexTmp = Utils.tempFileWith(indexFile)

    val indexFilePath = Utils.localFileToS3(shuffleS3Bucket, indexFile)
    val indexTmpPath = Utils.localFileToS3(shuffleS3Bucket, indexTmp)

    try {
      val outputStream = hadoopFileSystem.create(indexTmpPath)

      Utils.tryWithSafeFinally {
        var offset = 0L
        outputStream.writeLong(offset)
        for (length <- lengths) {
          offset += length
          outputStream.writeLong(offset)
        }
      } {
        outputStream.close()
      }

      val dataFile = getDataFile(shuffleId, mapId)
      val dataFilePath = Utils.localFileToS3(shuffleS3Bucket, dataFile)
      val dataTmpPath = Utils.localFileToS3(shuffleS3Bucket, dataTmp)
      // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
      // the following check and rename are atomic.
      synchronized {
        val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
        if (existingLengths != null) {
          // Another attempt for the same task has already written our map outputs successfully,
          // so just use the existing partition lengths and delete our temporary map outputs.
          System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
          if (dataTmpPath != null && hadoopFileSystem.exists(dataTmpPath)) {
            hadoopFileSystem.delete(dataTmpPath)
          }
          hadoopFileSystem.delete(indexTmpPath)
        } else {
          if (hadoopFileSystem.exists(indexFilePath)) {
            hadoopFileSystem.delete(indexFilePath)
          }
          if (hadoopFileSystem.exists(dataFilePath)) {
            hadoopFileSystem.delete(dataFilePath)
          }

          if (!hadoopFileSystem.rename(indexTmpPath, indexFilePath)) {
            throw new IOException(s"fail to rename hadoop path" +
              s" ${indexTmpPath} to ${indexFilePath}")
          }

          if (dataTmpPath != null && hadoopFileSystem.exists(dataTmpPath) &&
            !hadoopFileSystem.rename(dataTmpPath, dataFilePath)) {
            throw new IOException(s"fail to rename hadoop path " +
              s"${dataTmpPath} to ${dataFilePath}")
          }
        }
      }
    } finally {
      if (hadoopFileSystem.exists(indexTmpPath) && !hadoopFileSystem.delete(indexTmpPath)) {
        logError(s"Failed to delete temporary index file at ${indexTmpPath}")
      }
    }
  }

  def getRemoteBlockData(blockId: String, executorId: String): ManagedBuffer = {
    val blockIdParts = blockId.split("_")
    if (blockIdParts.length < 4) {
      throw new IllegalArgumentException("Unexpected block id format: " + blockId)
    } else if (!blockIdParts(0).equals("shuffle")) {
      throw new IllegalArgumentException("Expected shuffle block id, got: " + blockId)
    }

    val shuffleId = blockIdParts(1).toInt
    val mapId = blockIdParts(2).toInt
    val reduceId = blockIdParts(3).toInt

    val shuffleIndexFile = s"shuffle_${shuffleId}_${mapId}_0.index"
    val shuffleDataFile = s"shuffle_${shuffleId}_${mapId}_0.data"

    val localDirs = blockManager.diskBlockManager.localDirs.map(_.toString)
    val subDirs = blockManager.diskBlockManager.subDirsPerLocalDir

    val executorHashCode = Utils.nonNegativeHash(s"executor-${executorId}").toString.reverse
    val executorLocalDirs : Array[String] = localDirs.map { localDir => localDir.split("\\/")
      .dropRight(1)
      .mkString("/")
      .concat(s"/executor-${executorId}-${executorHashCode}")
    }

    val indexFile = getFile(executorLocalDirs, subDirs, shuffleIndexFile)
    val indexFilePath = Utils.localFileToS3(shuffleS3Bucket, indexFile)
    val in = hadoopFileSystem.open(indexFilePath)

    try {
      ByteStreams.skipFully(in, reduceId * 8)
      val offset = in.readLong()
      val nextOffset = in.readLong()
      val dataFile = getFile(executorLocalDirs, subDirs, shuffleDataFile)
      val dataFilePath = Utils.localFileToS3(shuffleS3Bucket, dataFile)

      logDebug("S3Segment managed buffer created for path - " + dataFilePath.toString)

      new S3SegmentManagedBuffer(
        transportConf,
        hadoopConf,
        dataFilePath,
        offset,
        nextOffset - offset)
    } finally {
      in.close()
    }
  }

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    getRemoteBlockData(blockId.toString, blockManager.blockManagerId.executorId)
  }

  private def getFile(localDirs: Array[String], subDirsPerLocalDir: Int, filename: String) = {
    val hash : Int = Utils.nonNegativeHash(filename)
    val localDir : String = localDirs(hash % localDirs.length)
    val subDirId : Int = (hash / localDirs.length) % subDirsPerLocalDir
    new File(new File(localDir, "%02x".format(subDirId)), filename)
  }

  override def stop(): Unit = {}
}

private[spark] object S3ShuffleBlockResolver {
  // No-op reduce ID used in interactions with disk store.
  // The disk store currently expects puts to relate to a (map, reduce) pair, but in the sort
  // shuffle outputs for several reduces are glommed into a single file.
  val NOOP_REDUCE_ID = 0
}
