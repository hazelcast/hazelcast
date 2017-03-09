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

package com.hazelcast.client.quorum;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.TestLoggingEntryProcessor;
import com.hazelcast.quorum.PartitionedCluster;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
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

import static com.hazelcast.client.quorum.QuorumTestUtil.getClientConfig;
import static com.hazelcast.map.InterceptorTest.SimpleInterceptor;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapWriteQuorumTest extends HazelcastTestSupport {

    private static final String MAP_NAME_PREFIX = "quorum";
    private static final String QUORUM_ID = "threeNodeQuorumRule";
    static PartitionedCluster cluster;

    static IMap<Object, Object> map1;
    static IMap<Object, Object> map2;
    static IMap<Object, Object> map3;
    static IMap<Object, Object> map4;
    static IMap<Object, Object> map5;

    static HazelcastInstance c1;
    static HazelcastInstance c2;
    static HazelcastInstance c3;
    static HazelcastInstance c4;
    static HazelcastInstance c5;

    private static TestHazelcastFactory factory;

    @BeforeClass
    public static void initialize() throws Exception {
        QuorumConfig quorumConfig = new QuorumConfig();
        quorumConfig.setName(QUORUM_ID);
        quorumConfig.setEnabled(true);
        quorumConfig.setSize(3);
        quorumConfig.setType(QuorumType.WRITE);
        MapConfig mapConfig = new MapConfig(MAP_NAME_PREFIX + "*");
        mapConfig.setQuorumName(QUORUM_ID);
        factory = new TestHazelcastFactory();
        cluster = new PartitionedCluster(factory).partitionFiveMembersThreeAndTwo(mapConfig, quorumConfig);
        initializeClients();
        verifyClients();
    }

    private static void initializeClients() {
        c1 = factory.newHazelcastClient(getClientConfig(cluster.h1));
        c2 = factory.newHazelcastClient(getClientConfig(cluster.h2));
        c3 = factory.newHazelcastClient(getClientConfig(cluster.h3));
        c4 = factory.newHazelcastClient(getClientConfig(cluster.h4));
        c5 = factory.newHazelcastClient(getClientConfig(cluster.h5));
    }

    private static void verifyClients() {
        assertClusterSizeEventually(3, c1);
        assertClusterSizeEventually(3, c2);
        assertClusterSizeEventually(3, c3);
        assertClusterSizeEventually(2, c4);
        assertClusterSizeEventually(2, c5);
    }

    @Before
    public void setUp() {
        String mapName = randomMapName(MAP_NAME_PREFIX);
        map1 = c1.getMap(mapName);
        map2 = c2.getMap(mapName);
        map3 = c3.getMap(mapName);
        map4 = c4.getMap(mapName);
        map5 = c5.getMap(mapName);
    }

    @AfterClass
    public static void killAllHazelcastInstances() {
        factory.terminateAll();
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
        Future<Object> foo = map1.putAsync("foo", "bar");
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testPutAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Object> foo = map4.putAsync("foo", "bar");
        foo.get();
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
        Future<Object> foo = map1.removeAsync("foo");
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testRemoveAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Object> foo = map4.removeAsync("foo");
        foo.get();
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
    public void testSubmmtToKeyOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future foo = map1.submitToKey("foo", new TestLoggingEntryProcessor());
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testSubmitToKeyOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future foo = map4.submitToKey("foo", new TestLoggingEntryProcessor());
        foo.get();
    }
}
