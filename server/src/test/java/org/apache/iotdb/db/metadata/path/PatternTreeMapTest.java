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
package org.apache.iotdb.db.metadata.path;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.metadata.path.PatternTreeMapFactory.ModsSerializer;
import org.apache.iotdb.db.metadata.path.PatternTreeMapFactory.StringSerializer;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PatternTreeMapTest {

  @Test
  public void stringAppendPatternTreeMapTest() throws IllegalPathException {
    PatternTreeMap<String, StringSerializer> patternTreeMap =
        PatternTreeMapFactory.getTriggerPatternTreeMap();

    patternTreeMap.append(new PartialPath("root.sg1.d1.s1"), "A");
    patternTreeMap.append(new PartialPath("root.**.s1"), "B");
    patternTreeMap.append(new PartialPath("root.sg1.*.s1"), "C");
    patternTreeMap.append(new PartialPath("root.sg1.d1.s1"), "D");
    patternTreeMap.append(new PartialPath("root.sg1.**"), "E");
    patternTreeMap.append(new PartialPath("root.sg1.**.s2"), "F");
    patternTreeMap.append(new PartialPath("root.**.d1.*"), "G");
    patternTreeMap.append(new PartialPath("root.**.d1.**"), "H");
    patternTreeMap.append(new PartialPath("root.*.d1.**"), "I");
    patternTreeMap.append(new PartialPath("root.**"), "J");
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s1"),
        new HashSet<>(Arrays.asList("A", "B", "C", "D", "E", "G", "H", "I", "J")));
    checkOverlapped(
        patternTreeMap, new PartialPath("root.sg2.s1"), new HashSet<>(Arrays.asList("B", "J")));
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s2"),
        new HashSet<>(Arrays.asList("E", "F", "G", "H", "I", "J")));
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.v1.s1"),
        new HashSet<>(Arrays.asList("B", "E", "H", "I", "J")));
    checkOverlappedByDeviceMeasurements(
        patternTreeMap,
        new PartialPath("root.sg1.d1"),
        Arrays.asList("s1", "s2"),
        Arrays.asList(
            new HashSet<>(Arrays.asList("A", "B", "C", "D", "E", "G", "H", "I", "J")),
            new HashSet<>(Arrays.asList("E", "F", "G", "H", "I", "J"))));
    checkOverlappedByDeviceMeasurements(
        patternTreeMap,
        new PartialPath("root.sg1.d2"),
        Arrays.asList("s1", "s2"),
        Arrays.asList(
            new HashSet<>(Arrays.asList("B", "C", "E", "G", "H", "I", "J")),
            new HashSet<>(Arrays.asList("E", "F", "G", "H", "I", "J"))));
    // delete leaf node with common parent
    patternTreeMap.delete(new PartialPath("root.**.d1.*"), "G");
    // only delete value, no delete leaf node
    patternTreeMap.delete(new PartialPath("root.sg1.d1.s1"), "D");
    // delete internal node
    patternTreeMap.delete(new PartialPath("root.**"), "J");
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s1"),
        new HashSet<>(Arrays.asList("A", "B", "C", "E", "H", "I")));
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s2"),
        new HashSet<>(Arrays.asList("E", "F", "H", "I")));
    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.v1.s1"),
        new HashSet<>(Arrays.asList("B", "E", "H", "I")));
    checkOverlappedByDeviceMeasurements(
        patternTreeMap,
        new PartialPath("root.sg1.d1"),
        Arrays.asList("s1", "s2"),
        Arrays.asList(
            new HashSet<>(Arrays.asList("A", "B", "C", "E", "H", "I")),
            new HashSet<>(Arrays.asList("E", "F", "H", "I"))));
    checkOverlappedByDeviceMeasurements(
        patternTreeMap,
        new PartialPath("root.sg1.d2"),
        Arrays.asList("s1", "s2"),
        Arrays.asList(
            new HashSet<>(Arrays.asList("B", "C", "E", "H", "I")),
            new HashSet<>(Arrays.asList("E", "F", "H", "I"))));
  }

  @Test
  public void modificationPatternTreeMapTest() throws IllegalPathException {
    PatternTreeMap<Modification, ModsSerializer> patternTreeMap =
        PatternTreeMapFactory.getModsPatternTreeMap();

    // [1,3] [6,10]
    patternTreeMap.append(
        new PartialPath("root.sg1.d1.s1"),
        new Deletion(new PartialPath("root.sg1.d1.s1"), 1, 1, 3));
    patternTreeMap.append(
        new PartialPath("root.sg1.d1.s1"),
        new Deletion(new PartialPath("root.sg1.d1.s1"), 1, 6, 10));

    patternTreeMap.append(
        new PartialPath("root.**.s1"), new Deletion(new PartialPath("root.**.s1"), 5, 10, 100));
    patternTreeMap.append(
        new PartialPath("root.**.s1"), new Deletion(new PartialPath("root.**.s1"), 10, 100, 200));

    patternTreeMap.append(
        new PartialPath("root.**"), new Deletion(new PartialPath("root.**"), 5, 10, 100));
    patternTreeMap.append(
        new PartialPath("root.**"), new Deletion(new PartialPath("root.**"), 5, 10, 100));

    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s1"),
        new HashSet<>(
            Arrays.asList(
                new Deletion(new PartialPath("root.sg1.d1.s1"), 1, 1, 3),
                new Deletion(new PartialPath("root.sg1.d1.s1"), 1, 6, 10),
                new Deletion(new PartialPath("root.**.s1"), 5, 10, 100),
                new Deletion(new PartialPath("root.**.s1"), 10, 100, 200),
                new Deletion(new PartialPath("root.**"), 5, 10, 100))));

    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d2.s1"),
        new HashSet<>(
            Arrays.asList(
                new Deletion(new PartialPath("root.**.s1"), 5, 10, 100),
                new Deletion(new PartialPath("root.**.s1"), 10, 100, 200),
                new Deletion(new PartialPath("root.**"), 5, 10, 100))));

    checkOverlapped(
        patternTreeMap,
        new PartialPath("root.sg1.d1.s2"),
        new HashSet<>(
            Collections.singletonList(new Deletion(new PartialPath("root.**"), 5, 10, 100))));
  }

  private <T> void checkOverlapped(
      PatternTreeMap<T, ?> patternTreeMap, PartialPath partialPath, Set<T> resultSet) {
    List<T> list = patternTreeMap.getOverlapped(partialPath);
    Assert.assertEquals(resultSet.size(), list.size());
    for (T o : list) {
      Assert.assertTrue(resultSet.contains(o));
    }
  }

  private <T> void checkOverlappedByDeviceMeasurements(
      PatternTreeMap<T, ?> patternTreeMap,
      PartialPath devicePath,
      List<String> measurements,
      List<Set<T>> resultSet) {
    List<List<T>> list = patternTreeMap.getOverlapped(devicePath, measurements);
    Assert.assertEquals(resultSet.size(), list.size());
    for (int i = 0; i < measurements.size(); i++) {
      List<T> subList = list.get(i);
      Set<T> subSet = resultSet.get(i);
      for (T o : subList) {
        Assert.assertTrue(subSet.contains(o));
      }
    }
  }
}
