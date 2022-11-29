/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block.stream;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.OutOfRangeException;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.io.LocalFileBlockReader;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * test stream.
 */
public class BlockDemoStream extends InputStream {

  /** The id of the block or UFS file to which this InStream provides access. */
  private final long mId;
  /** The size in bytes of the block. */
  private final long mLength;
  private String mPath;
  private final byte[] mSingleByte = new byte[1];

  /** Current position of the stream, relative to the start of the block. */
  private long mPos = 0;
  /** The current data chunk. */
  private DataBuffer mCurrentChunk;
  private long mLocalReaderChunkSize;

  private DataReader mDataReader;
//  private final DataReader.Factory mDataReaderFactory;

  private boolean mClosed = false;
  private boolean mEOF = false;

  /**
   * constructor.
   * @param filePath
   * @param blockId
   * @param length
   */
  public BlockDemoStream(String filePath, long blockId, long length) {
    mId = blockId;
    mPath = filePath;
    mLength = length;
    mLocalReaderChunkSize =
        Configuration.global().getBytes(PropertyKey.USER_LOCAL_READER_CHUNK_SIZE_BYTES);
  }

  @Override
  /**
   * read.
   */
  public int read() throws IOException {
    int bytesRead = read(mSingleByte);
    if (bytesRead == -1) {
      return -1;
    }
    Preconditions.checkState(bytesRead == 1);
    return BufferUtils.byteToInt(mSingleByte[0]);
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Objects.requireNonNull(b, "Read buffer cannot be null");
    return read(ByteBuffer.wrap(b), off, len);
  }

  /**
   * Reads up to len bytes of data from the input stream into the byte buffer.
   *
   * @param byteBuffer the buffer into which the data is read
   * @param off the start offset in the buffer at which the data is written
   * @param len the maximum number of bytes to read
   * @return the total number of bytes read into the buffer, or -1 if there is no more data because
   *         the end of the stream has been reached
   */
  public int read(ByteBuffer byteBuffer, int off, int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    if (mPos == mLength) {
      return -1;
    }
    readChunk();
    if (mCurrentChunk == null) {
      mEOF = true;
    }
    if (mEOF) {
      closeDataReader();
      if (mPos < mLength) {
        throw new OutOfRangeException(String.format("Block %s is expected to be %s bytes, "
                + "but only %s bytes are available in the UFS. "
                + "Please retry the read and on the next access, "
                + "Alluxio will sync with the UFS and fetch the updated file content.",
            mId, mLength, mPos));
      }
      return -1;
    }
    int toRead = Math.min(len, mCurrentChunk.readableBytes());
    byteBuffer.position(off).limit(off + toRead);
    mCurrentChunk.readBytes(byteBuffer);
    mPos += toRead;
    if (mPos == mLength) {
      // a performance improvement introduced by https://github.com/Alluxio/alluxio/issues/14020
      closeDataReader();
    }
    return toRead;
  }

  /**
   * Reads a new chunk from the channel if all of the current chunk is read.
   */
  private void readChunk() throws IOException {
    if (mDataReader == null) {
      LocalFileBlockReader reader = new LocalFileBlockReader(mPath);
      reader.increaseUsageCount();
      mDataReader = new LocalFileDataReader(reader, mPos, mLength - mPos, mLocalReaderChunkSize);
    }

    if (mCurrentChunk != null && mCurrentChunk.readableBytes() == 0) {
      mCurrentChunk.release();
      mCurrentChunk = null;
    }
    if (mCurrentChunk == null) {
      mCurrentChunk = mDataReader.readChunk();
    }
  }

  /**
   * Close the current data reader.
   */
  private void closeDataReader() throws IOException {
    if (mCurrentChunk != null) {
      mCurrentChunk.release();
      mCurrentChunk = null;
    }
    if (mDataReader != null) {
      mDataReader.close();
    }
    mDataReader = null;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    closeDataReader();
    mClosed = true;
  }
}
