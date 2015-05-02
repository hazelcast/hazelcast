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
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMapReadQuorumTest {

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
        quorumConfig.setType(QuorumType.READ);
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
    public void testGetOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.get("foo");
    }

    @Test(expected = QuorumException.class)
    public void testGetOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
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
    public void testGetAllOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        HashSet<Object> keys = new HashSet<Object>();
        keys.add("foo");
        map1.getAll(keys);
    }

    @Test(expected = QuorumException.class)
    public void testGetAllOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        HashSet<Object> keys = new HashSet<Object>();
        keys.add("foo");
        map4.getAll(keys);
    }

    @Test
    public void testGetEntryViewOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.getEntryView("foo");
    }

    @Test(expected = QuorumException.class)
    public void testGetEntryViewOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.getEntryView("foo");
    }


    @Test
    public void testContainsKeyOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.containsKey("foo");
    }

    @Test(expected = QuorumException.class)
    public void testContainsKeyOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.containsKey("foo");
    }

    @Test
    public void testContainsValueOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.containsValue("foo");
    }

    @Test(expected = QuorumException.class)
    public void testContainsValueOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.containsValue("foo");
    }

    @Test
    public void testKeySetOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.keySet();
    }

    @Test(expected = QuorumException.class)
    public void testKeySetOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.keySet();
    }

    @Test
    public void testValuesOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.values();
    }

    @Test(expected = QuorumException.class)
    public void testValuesOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.values();
    }

    @Test
    public void testEntrySetOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        map1.entrySet();
    }

    @Test(expected = QuorumException.class)
    public void testEntrySetOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        map4.entrySet();
    }


}
