package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * This test ensures that EntryListener on a map don't get lost on cluster changes.
 *
 * There is one client and an increasing/decreasing number of server instances. The clients puts the value 1 with a
 * constantly increased key (1, 2, 3, ...) into the map. The partition owner listens on the entryAdded event and adds
 * 1 to the event value. The client reacts on the entryUpdated event and checks if the event value is 2.
 *
 * The test checks for the number of received entryAdded and entryUpdated events and if the values are correct.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientMapEntryListenerOnClusterChangeTest extends HazelcastTestSupport {

    private static final int MAX_CLUSTER_SIZE = 10;
    private static final int CHANGE_CLUSTER_INTERVAL_MS = 1000;
    private static final int PRODUCER_PUT_INTERVAL_MS = 5;

    private AtomicInteger entriesSent;
    private AtomicInteger serverEntryListener;
    private AtomicInteger clientEntryListener;
    private ConcurrentMap<Long, Boolean> entriesPending;
    private ConcurrentMap<Long, Integer> entriesReceived;
    private ConcurrentMap<Long, Integer> entriesWrongValue;

    @Before
    public void setup() {
        entriesSent = new AtomicInteger(0);
        serverEntryListener = new AtomicInteger(0);
        clientEntryListener = new AtomicInteger(0);
        entriesPending = new ConcurrentHashMap<Long, Boolean>();
        entriesReceived = new ConcurrentHashMap<Long, Integer>();
        entriesWrongValue = new ConcurrentHashMap<Long, Integer>();
    }

    @After
    public void teardown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testMapEntryListenersDuringMigrationSingleNode() throws InterruptedException {
        testMapEntryListener(1, false, false);
    }

    @Test
    public void testMapEntryListenersDuringMigrationGrowCluster() throws InterruptedException {
        testMapEntryListener(MAX_CLUSTER_SIZE, false, false);
    }

    @Test
    public void testMapEntryListenersDuringMigrationShrinkClusterWithShutdown() throws InterruptedException {
        testMapEntryListener(MAX_CLUSTER_SIZE, true, false);
    }

    @Test
    public void testMapEntryListenersDuringMigrationShrinkClusterWithTerminate() throws InterruptedException {
        testMapEntryListener(MAX_CLUSTER_SIZE, true, true);
    }

    private void testMapEntryListener(int maxClusterSize, boolean shrinkCluster, boolean terminateServerInstances)
            throws InterruptedException {

        final String randomMapName = randomMapName();
        final List<HazelcastInstance> serverInstances = new ArrayList<HazelcastInstance>();

        // Start first server
        startServerInstance(serverInstances, randomMapName);

        // Start client
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();
        final IMap<Long, Integer> mapClient = client.getMap(randomMapName);
        mapClient.addEntryListener(new ClientEventListener(mapClient), true);

        // Start producer thread
        final ClientThread clientThread = new ClientThread(mapClient);
        clientThread.start();

        // Grow cluster to max size
        while (serverInstances.size() < maxClusterSize) {
            TimeUnit.MILLISECONDS.sleep(CHANGE_CLUSTER_INTERVAL_MS);

            startServerInstance(serverInstances, randomMapName);
        }

        // Check cluster size
        assertEquals(
                String.format("We should have %d cluster instances!", maxClusterSize),
                maxClusterSize,
                serverInstances.size()
        );
        for (HazelcastInstance server : serverInstances) {
            assertEquals(
                    String.format("Cluster size should be %d!", maxClusterSize),
                    maxClusterSize,
                    server.getCluster().getMembers().size()
            );
        }

        // Shrink cluster to single node
        if (shrinkCluster) {
            while (serverInstances.size() > 1) {
                TimeUnit.MILLISECONDS.sleep(CHANGE_CLUSTER_INTERVAL_MS);

                stopServerInstance(serverInstances, terminateServerInstances);
            }
        }

        // Stop client thread
        TimeUnit.MILLISECONDS.sleep(CHANGE_CLUSTER_INTERVAL_MS);
        clientThread.shutdown();
        clientThread.join();

        // Debug
        printDebugValues();

        // Check entries
        assertEquals("There should be 0 entries with wrong values!", 0, entriesWrongValue.size());
        assertEquals("There should be 0 pending entries!", 0, entriesPending.size());
        assertEquals(
                String.format("There should be %d received entries!", entriesSent.get()),
                entriesSent.get(),
                entriesReceived.size()
        );
        assertEquals(
                String.format("There should be %d client entry listener calls!", serverEntryListener.get()),
                serverEntryListener.get(),
                clientEntryListener.get()
        );
    }

    private void printDebugValues() {
        System.out.println("Sent entries: " + entriesSent.get());
        System.out.println("Received entries: " + entriesReceived.size());
        System.out.println("Server EntryListener: " + serverEntryListener.get());
        System.out.println("Client EntryListener: " + clientEntryListener.get());

        Object[] pendingEntries = entriesPending.keySet().toArray();
        Arrays.sort(pendingEntries);
        if (pendingEntries.length > 0) {
            System.out.println(String.format("Pending entries: %d", pendingEntries.length));

            for (Object entry : pendingEntries) {
                long key = (Long) entry;
                System.out.println(String.format("* key: %d server: %d", key, entriesReceived.get(key)));
            }
        }

        Object[] wrongEntries = entriesWrongValue.keySet().toArray();
        Arrays.sort(wrongEntries);
        if (wrongEntries.length > 0) {
            System.out.println(String.format("Wrong value entries: %d", wrongEntries.length));

            for (Object entry : wrongEntries) {
                long key = (Long) entry;
                System.out.println(String.format(
                                "* key: %d value: %s server: %d",
                                key,
                                entriesWrongValue.get(key),
                                entriesReceived.get(key))
                );
            }
        }
    }

    private void startServerInstance(List<HazelcastInstance> serverInstances, String mapName) {
        final HazelcastInstance server = Hazelcast.newHazelcastInstance();
        serverInstances.add(server);

        final IMap<Long, Integer> mapServer = server.getMap(mapName);
        mapServer.addEntryListener(new ServerEntryListener(serverInstances.size(), mapServer), true);
    }

    private void stopServerInstance(List<HazelcastInstance> serverInstances, boolean terminateServerInstances) {
        if (terminateServerInstances) {
            serverInstances.remove(0).getLifecycleService().terminate();
        } else {
            serverInstances.remove(0).shutdown();
        }
    }

    private class ServerEntryListener extends EntryAdapter<Long, Integer> {
        private final int instanceId;
        private final IMap<Long, Integer> serverMap;

        private ServerEntryListener(int instanceId, IMap<Long, Integer> serverMap) {
            this.instanceId = instanceId;
            this.serverMap = serverMap;
        }

        @Override
        public void entryAdded(EntryEvent<Long, Integer> event) {
            serverEntryListener.incrementAndGet();
            entriesReceived.put(event.getKey(), instanceId);
            serverMap.set(event.getKey(), event.getValue() + 1);
        }
    }

    private class ClientEventListener extends EntryAdapter<Long, Integer> {
        private final IMap<Long, Integer> mapClient;

        public ClientEventListener(IMap<Long, Integer> mapClient) {
            this.mapClient = mapClient;
        }

        @Override
        public void entryUpdated(EntryEvent<Long, Integer> event) {
            clientEntryListener.incrementAndGet();

            long key = event.getKey();
            entriesPending.remove(key);

            int value = mapClient.get(key);
            if (value != 2) {
                entriesWrongValue.put(key, value);
            }
        }
    }

    private class ClientThread extends Thread {
        private final IMap<Long, Integer> mapClient;

        private volatile boolean isRunning;

        public ClientThread(IMap<Long, Integer> mapClient) {
            this.mapClient = mapClient;

            isRunning = true;
        }

        @Override
        public void run() {
            long key = 0;
            while (isRunning) {
                try {
                    key++;
                    entriesSent.incrementAndGet();
                    entriesPending.put(key, true);
                    mapClient.set(key, 1);
                } catch (Exception e) {
                    System.err.println("Error while processing key " + key);
                    e.printStackTrace();

                    // Retry this key on the next loop
                    entriesPending.remove(key);
                    entriesSent.decrementAndGet();
                    key--;
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(PRODUCER_PUT_INTERVAL_MS);
                } catch (InterruptedException ignored) {
                }
            }
        }

        public void shutdown() {
            isRunning = false;
        }
    }
}