/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.encoding.decoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * This class is a decoder for decoding the byte array that encoded by {@code
 * DeltaBinaryEncoder}.DeltaBinaryDecoder just supports integer and long values.<br> .
 *
 * @see DeltaBinaryEncoder
 */
public abstract class DeltaBinaryDecoder extends Decoder {

  protected long count = 0;
  protected byte[] deltaBuf;

  /**
   * the first value in one pack.
   */
  protected int readIntTotalCount = 0;

  protected int nextReadIndex = 0;
  /**
   * max bit length of all value in a pack.
   */
  protected int packWidth;
  /**
   * data number in this pack.
   */
  protected int packNum;

  /**
   * how many bytes data takes after encoding.
   */
  protected int encodingLength;

  public DeltaBinaryDecoder() {
    super(TSEncoding.TS_2DIFF);
  }

  protected abstract void readHeader(ByteBuffer buffer) throws IOException;

  protected abstract void allocateDataArray();

  protected abstract void readValue(int i);

  /**
   * calculate the bytes length containing v bits.
   *
   * @param v - number of bits
   * @return number of bytes
   */
  protected int ceil(int v) {
    return (int) Math.ceil((double) (v) / 8.0);
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    return (nextReadIndex < readIntTotalCount) || buffer.remaining() > 0;
  }

  public static class IntDeltaDecoder extends DeltaBinaryDecoder {

    private int firstValue;
    private int[] data;
    private int previous;
    /**
     * minimum value for all difference.
     */
    private int minDeltaBase;

    public IntDeltaDecoder() {
      super();
    }

    /**
     * if there's no decoded data left, decode next pack into {@code data}.
     *
     * @param buffer ByteBuffer
     * @return int
     */
    protected int readT(ByteBuffer buffer) {
      if (nextReadIndex == readIntTotalCount) {
        return loadIntBatch(buffer);
      }
      return data[nextReadIndex++];
    }

    @Override
    public int readInt(ByteBuffer buffer) {
      return readT(buffer);
    }

    /**
     * if remaining data has been run out, load next pack from InputStream.
     *
     * @param buffer ByteBuffer
     * @return int
     */
    protected int loadIntBatch(ByteBuffer buffer) {
      packNum = ReadWriteIOUtils.readInt(buffer);
      packWidth = ReadWriteIOUtils.readInt(buffer);
      count++;
      readHeader(buffer);

      encodingLength = ceil(packNum * packWidth);
      deltaBuf = new byte[encodingLength];
      buffer.get(deltaBuf);
      allocateDataArray();

      previous = firstValue;
      readIntTotalCount = packNum;
      nextReadIndex = 0;
      readPack();
      return firstValue;
    }

    private void readPack() {
      for (int i = 0; i < packNum; i++) {
        readValue(i);
        previous = data[i];
      }
    }

    @Override
    protected void readHeader(ByteBuffer buffer) {
      minDeltaBase = ReadWriteIOUtils.readInt(buffer);
      firstValue = ReadWriteIOUtils.readInt(buffer);
    }

    @Override
    protected void allocateDataArray() {
      data = new int[packNum];
    }

    @Override
    protected void readValue(int i) {
      int v = BytesUtils.bytesToInt(deltaBuf, packWidth * i, packWidth);
      data[i] = previous + minDeltaBase + v;
    }

    @Override
    public void reset() {
      // do nothing
    }
  }

  public static class LongDeltaDecoder extends DeltaBinaryDecoder {

    private long firstValue;
    private long[] data;
    private long previous;
    /**
     * minimum value for all difference.
     */
    private long minDeltaBase;

    private boolean enableRegularityTimeDecode;
    private long regularTimeInterval;
    //    private byte[]
    //        encodedRegularTimeInterval; // it depends on minDeltaBase and bitWidth of each pack

    private Map<Pair<Long, Integer>, Map<Integer, byte[]>> allRegularBytes =
        new HashMap<>(); // <newRegularDelta,packWidth> -> (relativePos->bytes)

    private Map<Integer, int[]> allFallWithinMasks = new HashMap<>(); // packWidth -> fallWithinMasks

    public LongDeltaDecoder() {
      super();
      this.enableRegularityTimeDecode =
          TSFileDescriptor.getInstance().getConfig().isEnableRegularityTimeDecode();
      this.regularTimeInterval =
          TSFileDescriptor.getInstance().getConfig().getRegularTimeInterval();
    }

    /**
     * if there's no decoded data left, decode next pack into {@code data}.
     *
     * @param buffer ByteBuffer
     * @return long value
     */
    protected long readT(ByteBuffer buffer) {
      if (nextReadIndex == readIntTotalCount) {
        return loadIntBatch(buffer);
      }
      return data[nextReadIndex++];
    }

    /**
     * if remaining data has been run out, load next pack from InputStream.
     *
     * @param buffer ByteBuffer
     * @return long value
     */
    protected long loadIntBatch(ByteBuffer buffer) {
      long start = System.nanoTime();

      packNum = ReadWriteIOUtils.readInt(buffer);
      packWidth = ReadWriteIOUtils.readInt(buffer);
      count++;
      readHeader(buffer);

      previous = firstValue;
      readIntTotalCount = packNum;
      nextReadIndex = 0;

      encodingLength = ceil(packNum * packWidth);
      deltaBuf = new byte[encodingLength];
      buffer.get(deltaBuf);
      allocateDataArray();

      if (enableRegularityTimeDecode) {
        long newRegularDelta = regularTimeInterval - minDeltaBase;
        if (packWidth == 0) {
          // [CASE 1]
          for (int i = 0; i < packNum; i++) {
            data[i] = previous + minDeltaBase; // v=0
            previous = data[i];
          }
        } else if (newRegularDelta < 0 || newRegularDelta >= Math.pow(2, packWidth)) {
          // [CASE 2] no need to compare equality cause impossible
          for (int i = 0; i < packNum; i++) {
            long v = BytesUtils.bytesToLong(deltaBuf, packWidth * i, packWidth);
            data[i] = previous + minDeltaBase + v;
            previous = data[i];
          }
        } else {
          // [CASE 3]
          // preprocess to get fallWithinMasks and regularBytes
          int[] fallWithinMasks = null;
          if (packWidth >= 8) {
            fallWithinMasks = null;
          } else if (allFallWithinMasks.containsKey(packWidth)) {
            fallWithinMasks = allFallWithinMasks.get(packWidth);
          } else { // packWidth<8 and allFallWithinMasks does not contain it
            try {
              fallWithinMasks = TsFileConstant.generateFallWithinMasks(packWidth);
              allFallWithinMasks.put(packWidth, fallWithinMasks);
            } catch (Exception ignored) {
            }
          }
          Map<Integer, byte[]> regularBytes;
          if (allRegularBytes.containsKey(new Pair<>(newRegularDelta, packWidth))) {
            regularBytes = allRegularBytes.get(new Pair<>(newRegularDelta, packWidth));
          } else {  // TODO consider if the following steps can be accelerated by using bytes instead of bitwise get and set
            regularBytes = new HashMap<>();
            for (int i = 0; i < 8; i++) {
              // i is the starting position in the byte from high to low bits

              int endPos = i + packWidth - 1; // starting from 0
              int byteNum = endPos / 8 + 1;
              byte[] byteArray = new byte[byteNum];
              if (newRegularDelta != 0) {
                // put bit-packed newRegularDelta starting at position i,
                //  and pad the front and back with newRegularDeltas.
                // Otherwise if newRegularDelta=0, just leave byteArray as initial zeros

                // 1. deal with padding the first byte
                for (int x = i - 1; x >= 0; x--) {
                  // y is the position in the bit-packed newRegularDelta, 0->packWidth-1 from low to
                  // high bits
                  int y = (i - x - 1) % packWidth;
                  // get the bit indicated by y pos
                  int value = BytesUtils.getLongN(newRegularDelta, y);
                  // put the bit indicated by y pos into regularBytes
                  // setByte pos is from high to low starting from 0, corresponding to x
                  byteArray[0] = BytesUtils.setByteN(byteArray[0], x, value);
                }

                // 2. deal with putting newRegularDeltas
                BytesUtils.longToBytes(newRegularDelta, byteArray, i, packWidth);

                // 3. deal with padding the last byte
                for (int x = endPos + 1; x < byteNum * 8; x++) {
                  // y is the position in the bit-packed newRegularDelta, 0->packWidth-1 from low to
                  // high bits
                  int y = packWidth - 1 - (x - endPos - 1) % packWidth;
                  // get the bit indicated by y pos
                  int value = BytesUtils.getLongN(newRegularDelta, y);
                  // put the bit indicated by y pos into regularBytes
                  // setByte pos is from high to low starting from 0, corresponding to x
                  byteArray[byteNum - 1] = BytesUtils.setByteN(byteArray[byteNum - 1], x, value);
                }
              }
              regularBytes.put(i, byteArray);
            }
            allRegularBytes.put(new Pair<>(newRegularDelta, packWidth), regularBytes);
          }

          // Begin decoding each number in this pack
          for (int i = 0; i < packNum; i++) {
            //  (1) extract bytes from deltaBuf,
            //  (2) compare bytes with encodedRegularTimeInterval,
            //  (3) equal to reuse, else to convert

            boolean equal = true;

            int pos = i * packWidth
                % 8; // the starting relative position in the byte from high to low bits

            byte[] byteArray = regularBytes.get(pos); // the regular padded bytes to be compared

            int posByteIdx = i * packWidth / 8; // the start byte of the encoded new delta

            for (int k = 0; k < byteArray.length; k++, posByteIdx++) {
              byte regular = byteArray[k];
              byte data = deltaBuf[posByteIdx];
              if (regular != data) {
                equal = false;
                break;
              }
            }

            if (equal) {
              data[i] = previous + regularTimeInterval;
            } else {
              long v = BytesUtils.bytesToLong2(deltaBuf, packWidth * i, packWidth, fallWithinMasks);
              data[i] = previous + minDeltaBase + v;
            }
            previous = data[i];
          }
        }
      } else { // without regularity-aware decoding
        readPack();
      }

      long runTime = System.nanoTime() - start; // ns
      TsFileConstant.timeColumnTS2DIFFLoadBatchCost.addValue(runTime / 1000.0); // us

      return firstValue;
    }

    private void readPack() {
      for (int i = 0; i < packNum; i++) {
        readValue(i);
        previous = data[i];
      }
    }

    @Override
    public long readLong(ByteBuffer buffer) {

      return readT(buffer);
    }

    @Override
    protected void readHeader(ByteBuffer buffer) {
      minDeltaBase = ReadWriteIOUtils.readLong(buffer);
      firstValue = ReadWriteIOUtils.readLong(buffer);
    }

    @Override
    protected void allocateDataArray() {
      data = new long[packNum];
    }

    @Override
    protected void readValue(int i) {
      long v = BytesUtils.bytesToLong(deltaBuf, packWidth * i, packWidth);
      data[i] = previous + minDeltaBase + v;
    }

    @Override
    public void reset() {
      // do nothing
    }
  }
}
