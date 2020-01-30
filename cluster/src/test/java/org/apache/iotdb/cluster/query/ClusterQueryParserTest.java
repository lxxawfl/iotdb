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

package org.apache.iotdb.cluster.query;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.junit.Before;
import org.junit.Test;

public class ClusterQueryParserTest extends BaseQueryTest {

  private ClusterQueryParser parser;

  @Override
  @Before
  public void setUp() throws MetadataException {
    super.setUp();
    parser = new ClusterQueryParser(metaGroupMember);
  }

  @Test
  public void test() throws QueryProcessException {
    List<String> measurements = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      measurements.add(TestUtils.getTestMeasurement(i));
    }
    String sql = String.format("SELECT %s FROM %s", String.join(",", measurements),
        TestUtils.getTestSg(0));
    QueryPlan plan = (QueryPlan) parser.parseSQLToPhysicalPlan(sql);
    assertEquals(pathList, plan.getDeduplicatedPaths());
    assertEquals(dataTypes, plan.getDeduplicatedDataTypes());
  }
}