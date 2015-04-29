/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hazelcast.client.quorum;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.nio.Address;
import com.hazelcast.quorum.PartitionedCluster;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.InterceptorTest.SimpleInterceptor;
import static com.hazelcast.map.TempData.LoggingEntryProcessor;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMapWriteQuorumTest {

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

    private static final String MAP_NAME_PREFIX = "quorum";
    private static final String QUORUM_ID = "threeNodeQuorumRule";

    @BeforeClass
    public static void initialize() throws InterruptedException {
        QuorumConfig quorumConfig = new QuorumConfig();
        quorumConfig.setName(QUORUM_ID);
        quorumConfig.setEnabled(true);
        quorumConfig.setSize(3);
        quorumConfig.setType(QuorumType.WRITE);
        MapConfig mapConfig = new MapConfig(MAP_NAME_PREFIX + "*");
        mapConfig.setQuorumName(QUORUM_ID);
        cluster = new PartitionedCluster().partitionFiveMembersThreeAndTwo(mapConfig, quorumConfig);
        initializeClients();
    }

    private static void initializeClients() {
        c1 = HazelcastClient.newHazelcastClient(getClientConfig(cluster.h1));
        c2 = HazelcastClient.newHazelcastClient(getClientConfig(cluster.h2));
        c3 = HazelcastClient.newHazelcastClient(getClientConfig(cluster.h3));
        c4 = HazelcastClient.newHazelcastClient(getClientConfig(cluster.h4));
        c5 = HazelcastClient.newHazelcastClient(getClientConfig(cluster.h5));
    }

    private static ClientConfig getClientConfig(HazelcastInstance instance) {
        ClientConfig clientConfig = new ClientConfig();
        Address address = getNode(instance).address;
        clientConfig.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
        clientConfig.getGroupConfig().setName(instance.getConfig().getGroupConfig().getName());
        return clientConfig;
    }

    @Before
    public void setUp() throws Exception {
        String mapName = randomMapName(MAP_NAME_PREFIX);
        map1 = c1.getMap(mapName);
        map2 = c2.getMap(mapName);
        map3 = c3.getMap(mapName);
        map4 = c4.getMap(mapName);
        map5 = c5.getMap(mapName);
    }

    @AfterClass
    public static void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.terminateAll();
    }
    @Test
    public void testPutOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.put("foo", "bar");
    }

    @Test(expected = QuorumException.class)
    public void testPutOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.put("foo", "bar");
    }


    @Test
    public void testTryPutOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.tryPut("foo", "bar", 5, TimeUnit.SECONDS);
    }

    @Test(expected = QuorumException.class)
    public void testTryPutOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.tryPut("foo", "bar", 5, TimeUnit.SECONDS);
    }

    @Test
    public void testPutTransientOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.putTransient("foo", "bar", 5, TimeUnit.SECONDS);
    }

    @Test(expected = QuorumException.class)
    public void testPutTransientOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.putTransient("foo", "bar", 5, TimeUnit.SECONDS);
    }

    @Test
    public void testPutIfAbsentOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.putIfAbsent("foo", "bar");
    }

    @Test(expected = QuorumException.class)
    public void testPutIfAbsentOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
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
    public void testPutAllOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        HashMap<Object, Object> map = new HashMap<Object, Object>();
        map.put("foo", "bar");
        map1.putAll(map);
    }

    @Test(expected = QuorumException.class)
    public void testPutAllOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        HashMap<Object, Object> map = new HashMap<Object, Object>();
        map.put("foo", "bar");
        map4.putAll(map);
    }

    @Test
    public void testRemoveOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.remove("foo");
    }

    @Test(expected = QuorumException.class)
    public void testRemoveOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.remove("foo");
    }

    @Test
    public void testRemoveIfHasValueOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.remove("foo", "bar");
    }

    @Test(expected = QuorumException.class)
    public void testRemoveIfHasValueOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
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
    public void testDeleteOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.delete("foo");
    }

    @Test(expected = QuorumException.class)
    public void testDeleteOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.delete("foo");
    }

    @Test
    public void testClearOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.clear();
    }

    @Test(expected = QuorumException.class)
    public void testClearOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.clear();
    }

    @Test
    public void testSetOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.set("foo", "bar");
    }

    @Test(expected = QuorumException.class)
    public void testSetOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.set("foo", "bar");
    }

    @Test
    public void testReplaceOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.replace("foo", "bar");
    }

    @Test(expected = QuorumException.class)
    public void testReplaceOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.replace("foo", "bar");
    }

    @Test
    public void testReplaceIfOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.replace("foo", "bar", "baz");
    }

    @Test(expected = QuorumException.class)
    public void testReplaceIfOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.replace("foo", "bar", "baz");
    }

    @Test
    public void testTryRemoveOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.tryRemove("foo", 5, TimeUnit.SECONDS);
    }

    @Test(expected = QuorumException.class)
    public void testTryRemoveOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.tryRemove("foo", 5, TimeUnit.SECONDS);
    }

    @Test
    public void testFlushOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.flush();
    }

    @Test(expected = QuorumException.class)
    public void testFlushOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.flush();
    }

    @Test
    public void testEvictAllOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.evictAll();
    }

    @Test(expected = QuorumException.class)
    public void testEvictAllOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.evictAll();
    }

    @Test
    public void testEvictOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.evict("foo");
    }

    @Test(expected = QuorumException.class)
    public void testEvictOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.evict("foo");
    }

    @Test
    public void testAddIndexOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.addIndex("foo", false);
    }

    @Test(expected = QuorumException.class)
    public void testAddIndexOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.addIndex("foo", false);
    }

    @Test
    public void testAddInterceptorOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.addInterceptor(new SimpleInterceptor());
    }

    @Test(expected = QuorumException.class)
    public void testAddInterceptorOperationThrowsExceptionWhenQuorumSizeNotMet() {
        map4.addInterceptor(new SimpleInterceptor());
    }

    @Test
    public void testRemoveInterceptorOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.removeInterceptor("foo");
    }

    @Test(expected = QuorumException.class)
    public void testRemoveInterceptorOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.removeInterceptor("foo");
    }

    @Test
    public void testExecuteOnKeyOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.executeOnKey("foo", new LoggingEntryProcessor());
    }

    @Test(expected = QuorumException.class)
    public void testExecuteOnKeyOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.executeOnKey("foo", new LoggingEntryProcessor());
    }

    @Test
    public void testExecuteOnKeysOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        HashSet<Object> keys = new HashSet<Object>();
        keys.add("foo");
        map1.executeOnKey(keys, new LoggingEntryProcessor());
    }

    @Test(expected = QuorumException.class)
    public void testExecuteOnKeysOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        HashSet<Object> keys = new HashSet<Object>();
        keys.add("foo");
        map4.executeOnKey(keys, new LoggingEntryProcessor());
    }

    @Test
    public void testExecuteOnEntriesOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.executeOnEntries(new LoggingEntryProcessor());
    }

    @Test(expected = QuorumException.class)
    public void testExecuteOnEntriesOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.executeOnEntries(new LoggingEntryProcessor());
    }

    @Test
    public void testSubmmitToKeyOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future foo = map1.submitToKey("foo", new LoggingEntryProcessor());
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testSubmmitToKeyOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future foo = map4.submitToKey("foo", new LoggingEntryProcessor());
        foo.get();
    }


}
