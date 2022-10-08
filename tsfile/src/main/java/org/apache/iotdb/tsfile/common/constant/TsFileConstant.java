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

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

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

  public static DescriptiveStatistics timeColumnTS2DIFFLoadBatchCost = new DescriptiveStatistics();

  public static DescriptiveStatistics countForRegularEqual = new DescriptiveStatistics();
  public static DescriptiveStatistics countForRegularNOTEqual = new DescriptiveStatistics();
  public static DescriptiveStatistics countForRegularZero = new DescriptiveStatistics();

  public static DescriptiveStatistics countForRegularNewDeltas = new DescriptiveStatistics();

  public static DescriptiveStatistics countForHitNewDeltas = new DescriptiveStatistics();
  public static DescriptiveStatistics countForNotHitNewDeltas = new DescriptiveStatistics();

  public static DescriptiveStatistics byteArrayLengthStatistics = new DescriptiveStatistics();

  public static DescriptiveStatistics allRegularBytesSize = new DescriptiveStatistics();

  public static DescriptiveStatistics prepareAllRegulars = new DescriptiveStatistics();

  private TsFileConstant() {}
}
