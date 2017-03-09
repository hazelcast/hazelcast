/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.quorum.map;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.quorum.PartitionedCluster;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.test.HazelcastTestSupport.randomMapName;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapReadQuorumTest {

    private static final String MAP_NAME_PREFIX = "quorum";
    private static final String QUORUM_ID = "threeNodeQuorumRule";

    static PartitionedCluster cluster;
    static IMap<Object, Object> map1;
    static IMap<Object, Object> map2;
    static IMap<Object, Object> map3;
    static IMap<Object, Object> map4;
    static IMap<Object, Object> map5;

    @BeforeClass
    public static void initialize() throws Exception {
        QuorumConfig quorumConfig = new QuorumConfig();
        quorumConfig.setName(QUORUM_ID);
        quorumConfig.setType(QuorumType.READ);
        quorumConfig.setEnabled(true);
        quorumConfig.setSize(3);
        MapConfig mapConfig = new MapConfig(MAP_NAME_PREFIX + "*");
        mapConfig.setQuorumName(QUORUM_ID);
        cluster = new PartitionedCluster(new TestHazelcastInstanceFactory()).partitionFiveMembersThreeAndTwo(mapConfig, quorumConfig);
    }

    @Before
    public void setUp() {
        String mapName = randomMapName(MAP_NAME_PREFIX);
        map1 = cluster.h1.getMap(mapName);
        map2 = cluster.h2.getMap(mapName);
        map3 = cluster.h3.getMap(mapName);
        map4 = cluster.h4.getMap(mapName);
        map5 = cluster.h5.getMap(mapName);
    }

    @AfterClass
    public static void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testGetOperationSuccessfulWhenQuorumSizeMet() {
        map1.get("foo");
    }

    @Test(expected = QuorumException.class)
    public void testGetOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.get("foo");
    }

    @Test
    public void testGetAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<Object> foo = map1.getAsync("foo");
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testGetAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Object> foo = map4.getAsync("foo");
        foo.get();
    }

    @Test
    public void testGetAllOperationSuccessfulWhenQuorumSizeMet() {
        HashSet<Object> keys = new HashSet<Object>();
        keys.add("foo");
        map1.getAll(keys);
    }

    @Test(expected = QuorumException.class)
    public void testGetAllOperationThrowsExceptionWhenQuorumSizeNotMet() {
        HashSet<Object> keys = new HashSet<Object>();
        keys.add("foo");
        map4.getAll(keys);
    }

    @Test
    public void testGetEntryViewOperationSuccessfulWhenQuorumSizeMet() {
        map1.getEntryView("foo");
    }

    @Test(expected = QuorumException.class)
    public void testGetEntryViewOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.getEntryView("foo");
    }


    @Test
    public void testContainsKeyOperationSuccessfulWhenQuorumSizeMet() {
        map1.containsKey("foo");
    }

    @Test(expected = QuorumException.class)
    public void testContainsKeyOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.containsKey("foo");
    }

    @Test
    public void testContainsValueOperationSuccessfulWhenQuorumSizeMet() {
        map1.containsValue("foo");
    }

    @Test(expected = QuorumException.class)
    public void testContainsValueOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.containsValue("foo");
    }

    @Test
    public void testKeySetOperationSuccessfulWhenQuorumSizeMet() {
        map1.keySet();
    }

    @Test(expected = QuorumException.class)
    public void testKeySetOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.keySet();
    }

    @Test
    public void testLocalKeySetOperationSuccessfulWhenQuorumSizeMet() {
        map1.localKeySet();
    }

    @Test(expected = QuorumException.class)
    public void testLocalKeySetOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.localKeySet();
    }

    @Test
    public void testValuesOperationSuccessfulWhenQuorumSizeMet() {
        map1.values();
    }

    @Test(expected = QuorumException.class)
    public void testValuesOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.values();
    }

    @Test
    public void testEntrySetOperationSuccessfulWhenQuorumSizeMet() {
        map1.entrySet();
    }

    @Test(expected = QuorumException.class)
    public void testEntrySetOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.entrySet();
    }
}
