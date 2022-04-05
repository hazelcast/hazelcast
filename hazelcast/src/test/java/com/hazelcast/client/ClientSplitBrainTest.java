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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.test.Accessors.getClientEngineImpl;
import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static com.hazelcast.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class ClientSplitBrainTest extends ClientTestSupport {

    @After
    public void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testClientListeners_InSplitBrain() throws Throwable {
        Config config = new Config()
                .setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5")
                .setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

        final ClientConfig clientConfig = new ClientConfig();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final String mapName = randomMapName();
        final IMap mapNode1 = h1.getMap(mapName);
        final IMap mapNode2 = h2.getMap(mapName);
        final IMap mapClient = client.getMap(mapName);

        final AtomicBoolean[] listenerGotEventFlags = new AtomicBoolean[3];
        for (int i = 0; i < 3; i++) {
            listenerGotEventFlags[i] = new AtomicBoolean();
        }

        final CountDownLatch mergedLatch = new CountDownLatch(1);
        final LifecycleListener mergeListener = createMergeListener(mergedLatch);
        h1.getLifecycleService().addLifecycleListener(mergeListener);
        h2.getLifecycleService().addLifecycleListener(mergeListener);

        final EntryAdapter entryListener1 = createEntryListener(listenerGotEventFlags[0]);
        mapNode1.addEntryListener(entryListener1, true);

        final EntryAdapter entryListener2 = createEntryListener(listenerGotEventFlags[1]);
        mapNode2.addEntryListener(entryListener2, true);

        final EntryAdapter entryListener3 = createEntryListener(listenerGotEventFlags[2]);
        mapClient.addEntryListener(entryListener3, true);

        closeConnectionBetween(h2, h1);

        assertOpenEventually(mergedLatch);
        assertClusterSize(2, h1, h2);

        AtomicBoolean testFinished = new AtomicBoolean(false);
        final Thread clientThread = startClientPutThread(mapClient, testFinished);

        try {
            checkEventsEventually(listenerGotEventFlags);
        } finally {
            testFinished.set(true);
            clientThread.interrupt();
            clientThread.join();
        }
    }

    private void checkEventsEventually(final AtomicBoolean[] listenerGotEventFlags) {
        for (int i = 0; i < listenerGotEventFlags.length; i++) {
            final int id = i;
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    assertTrue("listener ID " + id, listenerGotEventFlags[id].get());
                }
            });
        }
    }

    private Thread startClientPutThread(final IMap<Object, Object> mapClient, final AtomicBoolean testFinished) {
        final Thread clientThread = new Thread() {
            @Override
            public void run() {
                while (!testFinished.get()) {
                    try {
                        mapClient.put(1, 1);
                    } catch (Throwable t) {
                        ignore(t);
                    }
                }
            }
        };

        clientThread.start();
        return clientThread;
    }

    private EntryAdapter createEntryListener(final AtomicBoolean listenerGotEventFlag) {
        return new EntryAdapter() {
            @Override
            public void onEntryEvent(EntryEvent event) {
                listenerGotEventFlag.set(true);
            }
        };
    }

    private LifecycleListener createMergeListener(final CountDownLatch mergedLatch) {
        return new LifecycleListener() {
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                    mergedLatch.countDown();
                }
            }
        };
    }

    @Test
    public void testClientEngineCleanup_AfterMergeFromSplitBrain() {
        Config config = new Config();
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "10");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "10");
        config.setProperty(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "5");
        config.setProperty(ClusterProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1");

        TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
        HazelcastInstance h1 = hazelcastFactory.newHazelcastInstance(config);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        HazelcastInstance h2 = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance h3 = hazelcastFactory.newHazelcastInstance(config);

        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);

        assertSizeEventually(3, clientInstanceImpl.getConnectionManager().getActiveConnections());

        blockCommunicationBetween(h1, h2);
        blockCommunicationBetween(h1, h3);

        // make sure that cluster is split as [ 1 ] , [ 2 , 3 ]
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(2, h2.getCluster().getMembers().size());
                assertEquals(2, h3.getCluster().getMembers().size());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, h1.getCluster().getMembers().size());
            }
        });

        // open communication back for nodes to merge
        unblockCommunicationBetween(h1, h2);
        unblockCommunicationBetween(h1, h3);

        // wait for cluster is merged back
        assertClusterSizeEventually(3, h1, h2, h3);

        // wait for client to connect back to all nodes
        assertSizeEventually(3, clientInstanceImpl.getConnectionManager().getActiveConnections());

        // verify endpoints are cleared
        ClientEngineImpl clientEngineImpl1 = getClientEngineImpl(h1);
        ClientEngineImpl clientEngineImpl2 = getClientEngineImpl(h2);
        ClientEngineImpl clientEngineImpl3 = getClientEngineImpl(h3);
        assertEquals(1, clientEngineImpl1.getClientEndpointCount());
        assertEquals(1, clientEngineImpl2.getClientEndpointCount());
        assertEquals(1, clientEngineImpl3.getClientEndpointCount());

        hazelcastFactory.shutdownAll();
    }
}
