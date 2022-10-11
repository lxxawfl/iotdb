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
package org.apache.iotdb.tsfile.common.constant;

import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TsFileConstant {

  public static final String TSFILE_SUFFIX = ".tsfile";
  public static final String TSFILE_HOME = "TSFILE_HOME";
  public static final String TSFILE_CONF = "TSFILE_CONF";
  public static final String PATH_ROOT = "root";
  public static final String TMP_SUFFIX = "tmp";
  public static final String PATH_SEPARATOR = ".";
  public static final char PATH_SEPARATOR_CHAR = '.';
  public static final String PATH_SEPARATER_NO_REGEX = "\\.";
  public static final char DOUBLE_QUOTE = '"';

  public static long timeColumnTS2DIFFLoadBatchCost = 0;

  public static long countForRegularEqual = 0; // equal to regular
  public static long countForRegularNOTEqual = 0; // not equal to regular
  public static long countForRegularZero = 0; // packWidth=0 no need

  public static DescriptiveStatistics regularNewDeltasStatistics = new DescriptiveStatistics();

  public static DescriptiveStatistics countForHitNewDeltas =
      new DescriptiveStatistics(); // allRegularBytes contains
  public static DescriptiveStatistics countForNotHitNewDeltas =
      new DescriptiveStatistics(); // allRegularBytes not contains

  public static DescriptiveStatistics byteArrayLengthStatistics =
      new DescriptiveStatistics(); // regular bytes length

  public static DescriptiveStatistics allRegularBytesSize =
      new DescriptiveStatistics(); // allRegularBytes map size

  public static DescriptiveStatistics prepareAllRegulars = new DescriptiveStatistics();

  public static long countLoadIntBatch = 0;

  public static long bytesToLong_byteNum1_wholeByte = 0;
  public static long bytesToLong_byteNum1_smallByte = 0;
  public static long byteToLong_byteNums_firstByte_wholeByte = 0;
  public static long byteToLong_byteNums_firstByte_smallByte = 0;
  public static long byteToLong_byteNums_lastByte_wholeByte = 0;
  public static long byteToLong_byteNums_lastByte_smallByte = 0;
  public static long byteToLong_byteNums_middleWholeByte = 0;

  public static int[] endInByteMasks = {
    0b01111111, 0b00111111, 0b00011111, 0b00001111, 0b00000111, 0b00000011, 0b00000001
  };

  public static int[] startInByteMasks = {
    0b10000000, 0b11000000, 0b11100000, 0b11110000, 0b11111000, 0b11111100, 0b11111110
  };

  /**
   * example: int[] fallWithinMasks_2 = { 0b11000000, 0b01100000, 0b00110000, 0b00011000,
   * 0b00001100, 0b00000110, 0b00000011 }; int[] fallWithinMasks_3 = { 0b11100000, 0b01110000,
   * 0b00111000, 0b00011100, 0b00001110, 0b00000111 };
   */
  public static int[] generateFallWithinMasks(int packWidth) throws IOException {
    if (packWidth >= 8) {
      throw new IOException("only accept packWidth smaller than 8.");
    }
    int num = 9 - packWidth;
    int[] res = new int[num];
    int mask = (int) Math.pow(2, packWidth) - 1;
    for (int i = num - 1; i >= 0; i--) {
      res[i] = mask << (num - 1 - i);
    }
    return res;
  }

  private TsFileConstant() {}

  /**
   * Pad the bytes with bit-packed newRegularDelta at 8 relative positions. This function is used
   * for regularity-aware decoding to compare regularBytes with newDeltaBytes. Example:
   * writeWidth=3, bit-packed newRegularDelta = 0b101, return regularBytes as: 0->0b10110110,
   * 1->0b11011011, 2->0b01101101, 3->0b10110110, 4->0b11011011, 5->0b01101101, 6->0b10110110 &
   * 0b11011011, 7->0b11011011 & 0b01101101.
   */
  public static byte[][] generateRegularByteArray(
      int writeWidth, long regularTimeInterval, long minDeltaBase) throws IOException {
    long newRegularDelta = regularTimeInterval - minDeltaBase;
    if (writeWidth == 0 || newRegularDelta < 0 || newRegularDelta >= Math.pow(2, writeWidth)) {
      throw new IOException(
          "writeWidth == 0 || newRegularDelta < 0 || newRegularDelta >= Math.pow(2, writeWidth)");
    }

    byte[][] regularBytes = new byte[8][]; // 8 relative positions. relativePos->bytes
    for (int i = 0; i < 8; i++) {
      // i is the starting position in the byte from high to low bits

      int endPos = i + writeWidth - 1; // starting from 0
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
          int y = (i - x - 1) % writeWidth;
          // get the bit indicated by y pos
          int value = BytesUtils.getLongN(newRegularDelta, y);
          // put the bit indicated by y pos into regularBytes
          // setByte pos is from high to low starting from 0, corresponding to x
          byteArray[0] = BytesUtils.setByteN(byteArray[0], x, value);
        }

        // 2. deal with putting newRegularDeltas
        BytesUtils.longToBytes(newRegularDelta, byteArray, i, writeWidth);

        // 3. deal with padding the last byte
        for (int x = endPos + 1; x < byteNum * 8; x++) {
          // y is the position in the bit-packed newRegularDelta, 0->packWidth-1 from low to
          // high bits
          int y = writeWidth - 1 - (x - endPos - 1) % writeWidth;
          // get the bit indicated by y pos
          int value = BytesUtils.getLongN(newRegularDelta, y);
          // put the bit indicated by y pos into regularBytes
          // setByte pos is from high to low starting from 0, corresponding to x
          byteArray[byteNum - 1] = BytesUtils.setByteN(byteArray[byteNum - 1], x, value);
        }
      }
      regularBytes[i] = byteArray;
    }
    return regularBytes;
  }

  public static byte[][] readRegularBytes(ByteBuffer buffer) {
    byte[][] regularBytes = new byte[8][];
    for (int i = 0; i < 8; i++) {
      int byteArrayLength = ReadWriteIOUtils.readInt(buffer);
      regularBytes[i] = new byte[byteArrayLength];
      for (int j = 0; j < byteArrayLength; j++) {
        regularBytes[i][j] = ReadWriteIOUtils.readByte(buffer);
      }
    }
    return regularBytes;
  }

  public static void printByteToLongStatistics() {
    System.out.println("bytesToLong_byteNum1_wholeByte=" + bytesToLong_byteNum1_wholeByte);
    System.out.println("bytesToLong_byteNum1_smallByte=" + bytesToLong_byteNum1_smallByte);
    System.out.println(
        "byteToLong_byteNums_firstByte_wholeByte=" + byteToLong_byteNums_firstByte_wholeByte);
    System.out.println(
        "byteToLong_byteNums_firstByte_smallByte=" + byteToLong_byteNums_firstByte_smallByte);
    System.out.println(
        "byteToLong_byteNums_lastByte_wholeByte=" + byteToLong_byteNums_lastByte_wholeByte);
    System.out.println(
        "byteToLong_byteNums_lastByte_smallByte=" + byteToLong_byteNums_lastByte_smallByte);
    System.out.println(
        "byteToLong_byteNums_middleWholeByte=" + byteToLong_byteNums_middleWholeByte);
  }
}
