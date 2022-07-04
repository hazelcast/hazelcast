/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestEndpointGroup;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.events.Event;
import com.hazelcast.internal.management.events.WanConsistencyCheckIgnoredEvent;
import com.hazelcast.internal.management.events.WanSyncIgnoredEvent;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.management.events.EventMetadata.EventType.WAN_CONSISTENCY_CHECK_IGNORED;
import static com.hazelcast.internal.management.events.EventMetadata.EventType.WAN_SYNC_IGNORED;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.TestEnvironment.HAZELCAST_TEST_USE_NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class WanOpenSourceAntiEntropyMcEventsTest extends HazelcastTestSupport {
    private static final String MAP_NAME = "map";
    private static final String WAN_REPLICATION_NAME = "wanRepName";
    private static final String WAN_PUBLISHER_ID = "wanPubId";

    private TestHazelcastInstanceFactory factory;

    @After
    public void tearDown() {
        factory.shutdownAll();
        System.clearProperty(HAZELCAST_TEST_USE_NETWORK);
    }

    @Test
    public void testConsistencyCheckAPI() {
        HazelcastInstance hz = createHazelcastInstance();

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        WanReplicationService wanService = nodeEngine.getWanReplicationService();
        ManagementCenterService mcService = nodeEngine.getManagementCenterService();

        List<Event> events = new LinkedList<>();
        CountDownLatch latch = new CountDownLatch(1);
        mcService.setEventListener(event -> {
            events.add(event);
            latch.countDown();
        });

        assertThrows(UnsupportedOperationException.class,
                () -> wanService.consistencyCheck(WAN_REPLICATION_NAME, WAN_PUBLISHER_ID, MAP_NAME));
        assertOpenEventually(latch);

        Event event = events.get(0);
        assertTrue(event instanceof WanConsistencyCheckIgnoredEvent);
        WanConsistencyCheckIgnoredEvent checkIgnoredEvent = (WanConsistencyCheckIgnoredEvent) event;
        assertNotNull(checkIgnoredEvent.getUuid());
        assertEquals(MAP_NAME, checkIgnoredEvent.getMapName());
    }

    @Test
    public void testConsistencyCheckREST() throws Exception {
        System.setProperty(HAZELCAST_TEST_USE_NETWORK, "true");

        HazelcastInstance hz = createHazelcastInstance(getConfigWithRest());
        HTTPCommunicator communicator = new HTTPCommunicator(hz);

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        ManagementCenterService mcService = nodeEngine.getManagementCenterService();

        List<Event> events = new LinkedList<>();
        CountDownLatch latch = new CountDownLatch(1);
        mcService.setEventListener(event -> {
            events.add(event);
            latch.countDown();
        });

        String jsonResult = communicator.wanMapConsistencyCheck(hz.getConfig().getClusterName(), "", WAN_REPLICATION_NAME,
                WAN_PUBLISHER_ID, MAP_NAME);
        assertOpenEventually(latch);

        JsonObject result = Json.parse(jsonResult).asObject();
        Event event = events.get(0);
        assertTrue(event instanceof WanConsistencyCheckIgnoredEvent);
        WanConsistencyCheckIgnoredEvent checkIgnoredEvent = (WanConsistencyCheckIgnoredEvent) event;
        assertNotNull(checkIgnoredEvent.getUuid());
        assertNull(result.getString("uuid", null));
        assertEquals(MAP_NAME, checkIgnoredEvent.getMapName());
        assertEquals(WAN_CONSISTENCY_CHECK_IGNORED, checkIgnoredEvent.getType());
    }

    @Test
    public void testSyncAPI() {
        HazelcastInstance hz = createHazelcastInstance();

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        WanReplicationService wanService = nodeEngine.getWanReplicationService();
        ManagementCenterService mcService = nodeEngine.getManagementCenterService();

        List<Event> events = new LinkedList<>();
        CountDownLatch latch = new CountDownLatch(1);
        mcService.setEventListener(event -> {
            events.add(event);
            latch.countDown();
        });

        assertThrows(UnsupportedOperationException.class,
                () -> wanService.syncMap(WAN_REPLICATION_NAME, WAN_PUBLISHER_ID, MAP_NAME));
        assertOpenEventually(latch);

        Event event = events.get(0);
        assertTrue(event instanceof WanSyncIgnoredEvent);
        WanSyncIgnoredEvent syncIgnoredEvent = (WanSyncIgnoredEvent) event;
        assertNotNull(syncIgnoredEvent.getUuid());
        assertEquals(MAP_NAME, syncIgnoredEvent.getMapName());
    }

    @Test
    public void testSyncREST() throws Exception {
        System.setProperty(HAZELCAST_TEST_USE_NETWORK, "true");

        HazelcastInstance hz = createHazelcastInstance(getConfigWithRest());
        HTTPCommunicator communicator = new HTTPCommunicator(hz);

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        ManagementCenterService mcService = nodeEngine.getManagementCenterService();

        List<Event> events = new LinkedList<>();
        CountDownLatch latch = new CountDownLatch(1);
        mcService.setEventListener(event -> {
            events.add(event);
            latch.countDown();
        });

        String jsonResult = communicator.syncMapOverWAN(hz.getConfig().getClusterName(), "", WAN_REPLICATION_NAME,
                WAN_PUBLISHER_ID, MAP_NAME);
        assertOpenEventually(latch);

        JsonObject result = Json.parse(jsonResult).asObject();
        Event event = events.get(0);
        assertTrue(event instanceof WanSyncIgnoredEvent);
        WanSyncIgnoredEvent syncIgnoredEvent = (WanSyncIgnoredEvent) event;
        assertNotNull(syncIgnoredEvent.getUuid());
        assertNull(result.getString("uuid", null));
        assertEquals(MAP_NAME, syncIgnoredEvent.getMapName());
        assertEquals(WAN_SYNC_IGNORED, syncIgnoredEvent.getType());
    }

    @Test
    public void testAllMapsSyncAPI() {
        HazelcastInstance hz = createHazelcastInstance();

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        WanReplicationService wanService = nodeEngine.getWanReplicationService();
        ManagementCenterService mcService = nodeEngine.getManagementCenterService();

        List<Event> events = new LinkedList<>();
        CountDownLatch latch = new CountDownLatch(1);
        mcService.setEventListener(event -> {
            events.add(event);
            latch.countDown();
        });

        assertThrows(UnsupportedOperationException.class,
                () -> wanService.syncAllMaps(WAN_REPLICATION_NAME, WAN_PUBLISHER_ID));
        assertOpenEventually(latch);

        Event event = events.get(0);
        assertTrue(event instanceof WanSyncIgnoredEvent);
        WanSyncIgnoredEvent syncIgnoredEvent = (WanSyncIgnoredEvent) event;
        assertNotNull(syncIgnoredEvent.getUuid());
        assertNull(syncIgnoredEvent.getMapName());
    }

    @Test
    public void testSyncAllREST() throws Exception {
        System.setProperty(HAZELCAST_TEST_USE_NETWORK, "true");

        HazelcastInstance hz = createHazelcastInstance(getConfigWithRest());
        HTTPCommunicator communicator = new HTTPCommunicator(hz);

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        ManagementCenterService mcService = nodeEngine.getManagementCenterService();

        List<Event> events = new LinkedList<>();
        CountDownLatch latch = new CountDownLatch(1);
        mcService.setEventListener(event -> {
            events.add(event);
            latch.countDown();
        });

        String jsonResult = communicator.syncMapsOverWAN(hz.getConfig().getClusterName(), "", WAN_REPLICATION_NAME,
                WAN_PUBLISHER_ID);
        assertOpenEventually(latch);

        JsonObject result = Json.parse(jsonResult).asObject();
        Event event = events.get(0);
        assertTrue(event instanceof WanSyncIgnoredEvent);
        WanSyncIgnoredEvent syncIgnoredEvent = (WanSyncIgnoredEvent) event;
        assertNotNull(syncIgnoredEvent.getUuid());
        assertNull(result.getString("uuid", null));
        assertNull(syncIgnoredEvent.getMapName());
        assertEquals(WAN_SYNC_IGNORED, syncIgnoredEvent.getType());
    }

    @Override
    protected HazelcastInstance createHazelcastInstance() {
        factory = new TestHazelcastInstanceFactory();
        return factory.newHazelcastInstance();
    }

    @Override
    protected HazelcastInstance createHazelcastInstance(Config config) {
        factory = new TestHazelcastInstanceFactory();
        return factory.newHazelcastInstance(config);
    }

    private Config getConfigWithRest() {
        Config config = smallInstanceConfig();
        RestApiConfig restApiConfig = config.getNetworkConfig().getRestApiConfig();
        restApiConfig.setEnabled(true);
        restApiConfig.enableGroups(RestEndpointGroup.WAN);
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig()
                .setEnabled(false);
        joinConfig.getTcpIpConfig()
                .setEnabled(true)
                .addMember("127.0.0.1");
        return config;
    }

}
