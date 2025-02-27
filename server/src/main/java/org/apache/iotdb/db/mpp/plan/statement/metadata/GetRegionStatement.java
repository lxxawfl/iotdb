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

package org.apache.iotdb.db.mpp.plan.statement.metadata;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * GET REGION statement
 *
 * <p>Here is the syntax definition:
 *
 * <p>SHOW (DATA|SCHEMA) REGIONID OF path=prefixPath WHERE SERIESSLOTID operator_eq
 * seriesSlot=INTEGER_LITERAL (OPERATOR_AND TIMESLOTID operator_eq timeSlot=INTEGER_LITERAL)?
 */
public class GetRegionStatement extends Statement implements IConfigStatement {

  private final String storageGroup;

  private final TSeriesPartitionSlot seriesSlotId;

  private final TConsensusGroupType partitionType;

  private TTimePartitionSlot timeSlotId;

  public GetRegionStatement(
      String storageGroup, TConsensusGroupType partitionType, TSeriesPartitionSlot seriesSlotId) {
    super();
    this.storageGroup = storageGroup;
    this.partitionType = partitionType;
    this.seriesSlotId = seriesSlotId;
  }

  public String getStorageGroup() {
    return storageGroup;
  }

  public TConsensusGroupType getPartitionType() {
    return partitionType;
  }

  public TSeriesPartitionSlot getSeriesSlotId() {
    return seriesSlotId;
  }

  public TTimePartitionSlot getTimeSlotId() {
    return timeSlotId;
  }

  public void setTimeSlotId(TTimePartitionSlot timeSlotId) {
    this.timeSlotId = timeSlotId;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitGetRegion(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.READ;
  }

  @Override
  public List<PartialPath> getPaths() {
    try {
      return Collections.singletonList(new PartialPath(storageGroup));
    } catch (IllegalPathException e) {
      return new ArrayList<>();
    }
  }
}
