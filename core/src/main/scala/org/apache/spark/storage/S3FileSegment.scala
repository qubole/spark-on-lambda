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

import java.io.File

import org.apache.hadoop.fs.Path

/**
 * References a particular segment of a S3 file (potentially the entire file),
 * based off an offset and a length.
 */
private[spark] class S3FileSegment(val path: Path,
                                   override val offset: Long,
                                   override val length: Long) extends
                     FileSegment(new File(s"file://${path.toUri}"),
                                 offset,
                                 length) {
  require(offset >= 0, s"File segment offset cannot be negative (got $offset)")
  require(length >= 0, s"File segment length cannot be negative (got $length)")
  override def toString: String = {
    "(name=%s, offset=%d, length=%d)".format(path.getName, offset, length)
  }
}
