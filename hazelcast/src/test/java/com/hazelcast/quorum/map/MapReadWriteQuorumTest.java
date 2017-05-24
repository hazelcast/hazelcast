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
import com.hazelcast.map.TestLoggingEntryProcessor;
import com.hazelcast.quorum.PartitionedCluster;
import com.hazelcast.quorum.QuorumException;
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.InterceptorTest.SimpleInterceptor;
import static com.hazelcast.quorum.PartitionedCluster.QUORUM_ID;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapReadWriteQuorumTest {

    private static final String MAP_NAME_PREFIX = "quorum";

    static PartitionedCluster cluster;

    IMap<Object, Object> map1;
    IMap<Object, Object> map2;
    IMap<Object, Object> map3;
    IMap<Object, Object> map5;
    IMap<Object, Object> map4;

    @BeforeClass
    public static void initialize() {
        QuorumConfig quorumConfig = new QuorumConfig();
        quorumConfig.setName(QUORUM_ID);
        quorumConfig.setEnabled(true);
        quorumConfig.setSize(3);
        MapConfig mapConfig = new MapConfig(MAP_NAME_PREFIX + "*");
        mapConfig.setQuorumName(QUORUM_ID);
        cluster = new PartitionedCluster(new TestHazelcastInstanceFactory())
                .partitionFiveMembersThreeAndTwo(mapConfig, quorumConfig);
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
    public void testPutOperationSuccessfulWhenQuorumSizeMet() {
        map1.put("foo", "bar");
    }

    @Test(expected = QuorumException.class)
    public void testPutOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.put("foo", "bar");
    }

    @Test
    public void testTryPutOperationSuccessfulWhenQuorumSizeMet() {
        map1.tryPut("foo", "bar", 5, TimeUnit.SECONDS);
    }

    @Test(expected = QuorumException.class)
    public void testTryPutOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.tryPut("foo", "bar", 5, TimeUnit.SECONDS);
    }

    @Test
    public void testPutTransientOperationSuccessfulWhenQuorumSizeMet() {
        map1.putTransient("foo", "bar", 5, TimeUnit.SECONDS);
    }

    @Test(expected = QuorumException.class)
    public void testPutTransientOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.putTransient("foo", "bar", 5, TimeUnit.SECONDS);
    }

    @Test
    public void testPutIfAbsentOperationSuccessfulWhenQuorumSizeMet() {
        map1.putIfAbsent("foo", "bar");
    }

    @Test(expected = QuorumException.class)
    public void testPutIfAbsentOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.putIfAbsent("foo", "bar");
    }

    @Test
    public void testPutAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<Object> future = map1.putAsync("foo", "bar");
        future.get();
    }

    @Test(expected = ExecutionException.class)
    public void testPutAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Object> future = map4.putAsync("foo", "bar");
        future.get();
    }

    @Test
    public void testPutAllOperationSuccessfulWhenQuorumSizeMet() {
        HashMap<Object, Object> map = new HashMap<Object, Object>();
        map.put("foo", "bar");
        map1.putAll(map);
    }

    @Test(expected = QuorumException.class)
    public void testPutAllOperationThrowsExceptionWhenQuorumSizeNotMet() {
        HashMap<Object, Object> map = new HashMap<Object, Object>();
        map.put("foo", "bar");
        map4.putAll(map);
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
        Future<Object> future = map1.getAsync("foo");
        future.get();
    }

    @Test(expected = ExecutionException.class)
    public void testGetAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Object> future = map4.getAsync("foo");
        future.get();
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
    public void testRemoveOperationSuccessfulWhenQuorumSizeMet() {
        map1.remove("foo");
    }

    @Test(expected = QuorumException.class)
    public void testRemoveOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.remove("foo");
    }

    @Test
    public void testRemoveIfHasValueOperationSuccessfulWhenQuorumSizeMet() {
        map1.remove("foo", "bar");
    }

    @Test(expected = QuorumException.class)
    public void testRemoveIfHasValueOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.remove("foo", "bar");
    }

    @Test
    public void testRemoveAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<Object> future = map1.removeAsync("foo");
        future.get();
    }

    @Test(expected = ExecutionException.class)
    public void testRemoveAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Object> future = map4.removeAsync("foo");
        future.get();
    }

    @Test
    public void testDeleteOperationSuccessfulWhenQuorumSizeMet() {
        map1.delete("foo");
    }

    @Test(expected = QuorumException.class)
    public void testDeleteOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.delete("foo");
    }

    @Test
    public void testClearOperationSuccessfulWhenQuorumSizeMet() {
        map1.clear();
    }

    @Test(expected = QuorumException.class)
    public void testClearOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.clear();
    }

    @Test
    public void testSetOperationSuccessfulWhenQuorumSizeMet() {
        map1.set("foo", "bar");
    }

    @Test(expected = QuorumException.class)
    public void testSetOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.set("foo", "bar");
    }

    @Test
    public void testReplaceOperationSuccessfulWhenQuorumSizeMet() {
        map1.replace("foo", "bar");
    }

    @Test(expected = QuorumException.class)
    public void testReplaceOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.replace("foo", "bar");
    }

    @Test
    public void testReplaceIfOperationSuccessfulWhenQuorumSizeMet() {
        map1.replace("foo", "bar", "baz");
    }

    @Test(expected = QuorumException.class)
    public void testReplaceIfOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.replace("foo", "bar", "baz");
    }

    @Test
    public void testTryRemoveOperationSuccessfulWhenQuorumSizeMet() {
        map1.tryRemove("foo", 5, TimeUnit.SECONDS);
    }

    @Test(expected = QuorumException.class)
    public void testTryRemoveOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.tryRemove("foo", 5, TimeUnit.SECONDS);
    }

    @Test
    public void testFlushOperationSuccessfulWhenQuorumSizeMet() {
        map1.flush();
    }

    @Test(expected = QuorumException.class)
    public void testFlushOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.flush();
    }

    @Test
    public void testEvictAllOperationSuccessfulWhenQuorumSizeMet() {
        map1.evictAll();
    }

    @Test(expected = QuorumException.class)
    public void testEvictAllOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.evictAll();
    }

    @Test
    public void testEvictOperationSuccessfulWhenQuorumSizeMet() {
        map1.evict("foo");
    }

    @Test(expected = QuorumException.class)
    public void testEvictOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.evict("foo");
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

    @Test
    public void testAddIndexOperationSuccessfulWhenQuorumSizeMet() {
        map1.addIndex("foo", false);
    }

    @Test(expected = QuorumException.class)
    public void testAddIndexOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.addIndex("foo", false);
    }

    @Test
    public void testAddInterceptorOperationSuccessfulWhenQuorumSizeMet() {
        map1.addInterceptor(new SimpleInterceptor());
    }

    @Test(expected = QuorumException.class)
    public void testAddInterceptorOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.addInterceptor(new SimpleInterceptor());
    }

    @Test
    public void testRemoveInterceptorOperationSuccessfulWhenQuorumSizeMet() {
        map1.removeInterceptor("foo");
    }

    @Test(expected = QuorumException.class)
    public void testRemoveInterceptorOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.removeInterceptor("foo");
    }

    @Test
    public void testExecuteOnKeyOperationSuccessfulWhenQuorumSizeMet() {
        map1.executeOnKey("foo", new TestLoggingEntryProcessor());
    }

    @Test(expected = QuorumException.class)
    public void testExecuteOnKeyOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.executeOnKey("foo", new TestLoggingEntryProcessor());
    }

    @Test
    public void testExecuteOnKeysOperationSuccessfulWhenQuorumSizeMet() {
        HashSet<Object> keys = new HashSet<Object>();
        keys.add("foo");
        map1.executeOnKey(keys, new TestLoggingEntryProcessor());
    }

    @Test(expected = QuorumException.class)
    public void testExecuteOnKeysOperationThrowsExceptionWhenQuorumSizeNotMet() {
        HashSet<Object> keys = new HashSet<Object>();
        keys.add("foo");
        map4.executeOnKey(keys, new TestLoggingEntryProcessor());
    }

    @Test
    public void testExecuteOnEntriesOperationSuccessfulWhenQuorumSizeMet() {
        map1.executeOnEntries(new TestLoggingEntryProcessor());
    }

    @Test(expected = QuorumException.class)
    public void testExecuteOnEntriesOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.executeOnEntries(new TestLoggingEntryProcessor());
    }

    @Test
    public void testSubmitToKeyOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future future = map1.submitToKey("foo", new TestLoggingEntryProcessor());
        future.get();
    }

    @Test(expected = ExecutionException.class)
    public void testSubmitToKeyOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future future = map4.submitToKey("foo", new TestLoggingEntryProcessor());
        future.get();
    }
}
