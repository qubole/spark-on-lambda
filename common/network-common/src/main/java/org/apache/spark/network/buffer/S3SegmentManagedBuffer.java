package org.apache.spark.network.buffer;

import com.google.common.base.Objects;
import io.netty.buffer.Unpooled;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.network.util.TransportConf;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by venkata on 5/1/17.
 */
public class S3SegmentManagedBuffer extends ManagedBuffer {
  private final TransportConf conf;
  private final Path file;
  private final long offset;
  private final long length;
  private FileSystem fs;
  private Configuration hadoopConf;

  public S3SegmentManagedBuffer(TransportConf conf, Configuration hadoopConf, Path file, long offset, long length)
          throws IOException {
    this.conf = conf;
    this.file = file;
    this.offset = offset;
    this.length = length;
    this.hadoopConf = hadoopConf;
    this.fs = file.getFileSystem(hadoopConf);
  }

  @Override
  public long size() {
    return length;
  }

  // TODO: BHARATH: All 3 functions in this file are identical except for one line which is the
  // returned wrapper. Can we refactor into one?
  @Override
  public ByteBuffer nioByteBuffer() throws IOException {
    FSDataInputStream inputStream = seekToOffset(offset);
    return ByteBuffer.wrap(IOUtils.toByteArray(inputStream.getWrappedStream()));
  }

  @Override
  public InputStream createInputStream() throws IOException {
    FSDataInputStream inputStream = seekToOffset(offset);
    return new LimitedInputStream(inputStream.getWrappedStream(), length);
  }

  @Override
  public ManagedBuffer retain() {
    return this;
  }

  @Override
  public ManagedBuffer release() {
    return this;
  }

  @Override
  public Object convertToNetty() throws IOException {
    FSDataInputStream inputStream = seekToOffset(offset);
    return Unpooled.wrappedBuffer(IOUtils.toByteArray(inputStream.getWrappedStream()));
  }

  private FSDataInputStream seekToOffset(long offset) throws IOException {
    FSDataInputStream inputStream = null;
    try {
      // TODO: BHARATH: Check if we can use seek of FileSystem itself.
      inputStream = fs.open(file);
      // TODO: BHARATH: Check that no double copy happens here. We should be able to use the underlying
      // S3A buffer.
      // TODO: BHARATH: Check if we can create these buffers outside heap like tungsten.
      inputStream.seek(offset);
      return inputStream;
    } catch (IOException e) {
      try {
        if (inputStream != null) {
          long size = -1;
          throw new IOException("Error in reading " + this + " (actual file length " + size + ")",
                  e);
        }
      } catch (IOException ignored) {
        // ignore
      } finally {
        JavaUtils.closeQuietly(inputStream);
      }
      throw new IOException("Error in opening " + this, e);
    } catch (RuntimeException e) {
      JavaUtils.closeQuietly(inputStream);
      throw e;
    }
  }

  public Path getFile() { return file; }

  public long getOffset() { return offset; }

  public long getLength() { return length; }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
            .add("file", file)
            .add("offset", offset)
            .add("length", length)
            .toString();
  }
}
