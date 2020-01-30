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

package org.apache.iotdb.cluster.log.logtypes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.Test;

public class SerializeLogTest {

  @Test
  public void testPhysicalPlanLog() throws UnknownLogTypeException {
    PhysicalPlanLog log = new PhysicalPlanLog();
    InsertPlan plan = new InsertPlan();
    plan.setDeviceId("root.d1");
    plan.setMeasurements(new String[]{"s1,s2,s3"});
    plan.setDataTypes(new TSDataType[]{TSDataType.DOUBLE, TSDataType.INT64, TSDataType.TEXT});
    plan.setValues(new String[] {"0.1", "1", "\"dd\""});
    plan.setTime(1);
    log.setPlan(plan);

    ByteBuffer byteBuffer = log.serialize();
    Log logPrime = LogParser.getINSTANCE().parse(byteBuffer);
    assertEquals(log, logPrime);

    log = new PhysicalPlanLog(new SetStorageGroupPlan(new Path("root.sg1")));
    byteBuffer = log.serialize();
    logPrime = LogParser.getINSTANCE().parse(byteBuffer);
    assertEquals(log, logPrime);
  }

  @Test
  public void testAddNodeLog() throws UnknownLogTypeException {
    AddNodeLog log = new AddNodeLog();
    log.setNewNode(new Node("apache.iotdb.com", 1234, 1, 4321));
    ByteBuffer byteBuffer = log.serialize();
    Log logPrime = LogParser.getINSTANCE().parse(byteBuffer);
    assertEquals(log, logPrime);
  }

  @Test
  public void testCloseFileLog() throws UnknownLogTypeException {
    CloseFileLog log = new CloseFileLog("root.sg1", true);
    ByteBuffer byteBuffer = log.serialize();
    CloseFileLog logPrime = (CloseFileLog) LogParser.getINSTANCE().parse(byteBuffer);
    assertTrue(logPrime.isSeq());
    assertEquals("root.sg1", logPrime.getStorageGroupName());
    assertEquals(log, logPrime);
  }

}