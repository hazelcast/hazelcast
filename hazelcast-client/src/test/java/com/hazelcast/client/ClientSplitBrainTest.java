package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.util.EmptyStatement;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class ClientSplitBrainTest extends HazelcastTestSupport {

    @After
    public void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testClientListeners_InSplitBrain() throws Throwable {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "5");
        config.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "5");
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

        assertTrue(mergedLatch.await(30, TimeUnit.SECONDS));
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());

        AtomicBoolean testFinished = new AtomicBoolean(false);
        final Thread clientThread = startClientPutThread(mapClient, testFinished);

        try {
            checkEventsEventually(listenerGotEventFlags);
        } catch (Throwable t) {
            throw t;
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
                public void run() throws Exception {
                    assertTrue("listener id " + id, listenerGotEventFlags[id].get());
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
                        EmptyStatement.ignore(t);
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

}
