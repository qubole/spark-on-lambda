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
package org.apache.spark.storage

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.spark.SparkConf
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{JavaSerializer, SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.util.Utils

class S3BlockObjectWriter(file: File,
                          path: Path,
                          hadoopConf: Configuration,
                          serializerManager: SerializerManager,
                          serializerInstance: SerializerInstance,
                          bufferSize: Int,
                          syncWrites: Boolean,
                          // These write metrics concurrently shared with other active
                          // DiskBlockObjectWriters who
                          // are themselves performing writes. All updates must be relative.
                          writeMetrics: ShuffleWriteMetrics,
                          override val blockId: BlockId = null)
  extends DiskBlockObjectWriter(file,
                                serializerManager,
                                serializerInstance,
                                bufferSize,
                                syncWrites,
                                writeMetrics,
                                blockId) with Logging {

  private var fsOutputStream: FSDataOutputStream = null
  private var mcs: ManualCloseOutputStream = null
  private var bs: OutputStream = null
  private var fos: FileOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null
  private var initialized = false
  private var streamOpen = false
  private var hasBeenClosed = false
  private val DEFAULT_BLOCKSIZE = 128 * 1024 * 1024;

  /**
    * Cursors used to represent positions in the file.
    *
    * xxxxxxxxxx|----------|-----|
    *           ^          ^     ^
    *           |          |    channel.position()
    *           |        reportedPosition
    *         committedPosition
    *
    * reportedPosition: Position at the time of the last update to the write metrics.
    * committedPosition: Offset after last committed write.
    * -----: Current writes to the underlying file.
    * xxxxx: Committed contents of the file.
    */
  private var committedPosition = 0L
  private var reportedPosition = committedPosition

  /**
    * Guards against close calls, e.g. from a wrapping stream.
    * Call manualClose to close the stream that was extended by this trait.
    * Commit uses this trait to close object streams without paying the
    * cost of closing and opening the underlying file.
    */

  /**
    * Keep track of number of records written and also use this to periodically
    * output bytes written since the latter is expensive to do for each record.
    */
  private var numRecordsWritten = 0

  private trait ManualCloseOutputStream extends OutputStream {
    abstract override def close(): Unit = {
      flush()
    }

    def manualClose(): Unit = {
      super.close()
    }
  }

  private def initialize(): Unit = {
    logInfo(s"File - ${path.toString} getting initialized");
    // TODO: Revisit this back. FilePermissions, instantiating S3OutputStream
    val fileSystem = path.getFileSystem(hadoopConf)
    logInfo(s"Filesystem type - ${fileSystem.getClass.getName}")
    val permission = new FsPermission(777.toShort)
    fsOutputStream = fileSystem.create(path, permission, true, 4096,
      1.toShort, DEFAULT_BLOCKSIZE.toLong, null)
    ts = new TimeTrackingOutputStream(writeMetrics, fsOutputStream)
    class ManualCloseBufferedOutputStream
      extends BufferedOutputStream(ts, bufferSize) with ManualCloseOutputStream
    mcs = new ManualCloseBufferedOutputStream
  }

  override def open(): S3BlockObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    if (!initialized) {
      initialize()
      initialized = true
    }

    bs = serializerManager.wrapStream(blockId, mcs)
    objOut = serializerInstance.serializeStream(bs)
    streamOpen = true
    this
  }

  /**
    * Close and cleanup all resources.
    * Should call after committing or reverting partial writes.
    */
  private def closeResources(): Unit = {
    if (initialized) {
      mcs.manualClose()
      mcs = null
      bs = null
      ts = null
      objOut = null
      initialized = false
      streamOpen = false
      hasBeenClosed = true
    }
  }

  /**
    * Commits any remaining partial writes and closes resources.
    */
  override def close() {
    if (initialized) {
      Utils.tryWithSafeFinally {
        commitAndGet()
      } {
        closeResources()
      }
    }
  }

  /**
    * Flush the partial writes and commit them as a single atomic block.
    * A commit may write additional bytes to frame the atomic block.
    *
    * @return file segment with previous offset and length committed on this call.
    */
  override def commitAndGet(): S3FileSegment = {
    if (streamOpen) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      bs.flush()
      objOut.close()
      streamOpen = false

      val pos = fsOutputStream.getPos
      val fileSegment = new S3FileSegment(path, committedPosition, pos - committedPosition)
      committedPosition = pos
      // In certain compression codecs, more bytes are written after streams are closed
      writeMetrics.incBytesWritten(committedPosition - reportedPosition)
      reportedPosition = committedPosition
      fileSegment
    } else {
      new S3FileSegment(path, committedPosition, 0)
    }
  }

  /**
    * Writes a key-value pair.
    */
  override def write(key: Any, value: Any) {
    if (!streamOpen) {
      open()
    }

    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
  }

  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (!streamOpen) {
      open()
    }

    bs.write(kvBytes, offs, len)
  }

  /**
    * Notify the writer that a record worth of bytes has been written with OutputStream#write.
    */
  override def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)

   if (numRecordsWritten % 16384 == 0) {
      updateBytesWritten()
    }
  }

  /**
    * Report the number of bytes written in this writer's shuffle write metrics.
    * Note that this is only valid before the underlying streams are closed.
    */
  private def updateBytesWritten() {
    val pos = fsOutputStream.getPos
    writeMetrics.incBytesWritten(pos - reportedPosition)
    reportedPosition = pos
  }

  // For testing
  private[spark] override def flush() {
    objOut.flush()
    bs.flush()
  }
}

  object S3BlockObjectWriter {
    // For testing
    def createWriter(path: Path,
                     conf: SparkConf,
                     hadoopConf: Configuration) : S3BlockObjectWriter = {
      val serializerManager = new SerializerManager(new JavaSerializer(conf), conf)
      val writeMetrics = new ShuffleWriteMetrics()
      new S3BlockObjectWriter(new File(path.toUri), path, hadoopConf,
        serializerManager, new JavaSerializer(conf).newInstance(), 1024, false, writeMetrics)
  }
}
