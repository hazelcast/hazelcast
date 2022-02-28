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

package com.hazelcast.client.map;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.PartitionLostEventImpl;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.TestEventCollectingMapPartitionLostListener;
import com.hazelcast.map.impl.MapPartitionLostEventFilter;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.proxyservice.InternalProxyService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapPartitionLostListenerTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void test_mapPartitionLostListener_registered() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(getConfig());
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig());

        String mapName = randomMapName();

        client.getMap(mapName).addPartitionLostListener(mock(MapPartitionLostListener.class));

        assertRegistrationEventually(instance, mapName, true);
    }

    @Test
    public void test_mapPartitionLostListener_removed() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(getConfig());
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig());

        String mapName = randomMapName();

        UUID registrationId = client.getMap(mapName).addPartitionLostListener(mock(MapPartitionLostListener.class));
        assertRegistrationEventually(instance, mapName, true);

        assertTrue(client.getMap(mapName).removePartitionLostListener(registrationId));
        assertRegistrationEventually(instance, mapName, false);
    }

    @Test
    public void test_mapPartitionLostListener_invoked() {
        String mapName = randomMapName();
        Config config = getConfig();
        config.getMapConfig(mapName).setBackupCount(0);
        ClientConfig clientConfig = getClientConfig();

        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        warmUpPartitions(instance, client);

        TestEventCollectingMapPartitionLostListener listener = new TestEventCollectingMapPartitionLostListener(0);
        client.getMap(mapName).addPartitionLostListener(listener);

        MapService mapService = getNode(instance).getNodeEngine().getService(MapService.SERVICE_NAME);
        int partitionId = 5;
        mapService.onPartitionLost(new PartitionLostEventImpl(partitionId, 0, null));

        assertMapPartitionLostEventEventually(listener, partitionId);
    }

    @Test
    public void test_mapPartitionLostListener_invoked_fromOtherNode() {
        String mapName = randomMapName();
        Config config = getConfig();
        config.getMapConfig(mapName).setBackupCount(0);

        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance(config);
        ClientConfig clientConfig = getClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance(config);

        TestEventCollectingMapPartitionLostListener listener = new TestEventCollectingMapPartitionLostListener(0);
        client.getMap(mapName).addPartitionLostListener(listener);

        assertRegistrationEventually(instance1, mapName, true);
        assertRegistrationEventually(instance2, mapName, true);

        assertProxyExistsEventually(instance1, mapName);
        assertProxyExistsEventually(instance2, mapName);

        MapService mapService = getNode(instance2).getNodeEngine().getService(SERVICE_NAME);
        int partitionId = 5;
        mapService.onPartitionLost(new PartitionLostEventImpl(partitionId, 0, null));

        assertMapPartitionLostEventEventually(listener, partitionId);
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    private static void assertMapPartitionLostEventEventually(final TestEventCollectingMapPartitionLostListener listener,
                                                              final int partitionId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {

                final List<MapPartitionLostEvent> events = listener.getEvents();
                assertFalse(events.isEmpty());
                assertEquals(partitionId, events.get(0).getPartitionId());

            }
        });
    }

    private static void assertProxyExistsEventually(HazelcastInstance instance, final String proxyName) {
        final InternalProxyService proxyService = getNodeEngineImpl(instance).getProxyService();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Collection<DistributedObject> allDistributedObjects = proxyService.getAllDistributedObjects();
                for (DistributedObject distributedObject : allDistributedObjects) {
                    if (distributedObject.getName().equals(proxyName)) {
                        return;
                    }
                }
                fail("There is no proxy with name " + proxyName + " created (yet)");
            }
        });
    }

    private static void assertRegistrationEventually(final HazelcastInstance instance, final String mapName,
                                                     final boolean shouldBeRegistered) {
        final EventService eventService = getNode(instance).getNodeEngine().getEventService();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                boolean registered = false;
                for (EventRegistration registration : eventService.getRegistrations(SERVICE_NAME, mapName)) {
                    if (registration.getFilter() instanceof MapPartitionLostEventFilter) {
                        registered = true;
                        break;
                    }
                }
                if (shouldBeRegistered != registered) {
                    fail("shouldBeRegistered: " + shouldBeRegistered + " registered: " + registered);
                }
            }
        });
    }
}
