package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.partition.AbstractPartitionLostListenerTest;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestPartitionUtils;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.instance.GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS;
import static com.hazelcast.map.nearcache.NearCacheLocalImmediateInvalidateTest.mapName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapPartitionLostListenerStressTest
        extends AbstractPartitionLostListenerTest {

    public static class EventCollectingMapPartitionLostListener
            implements MapPartitionLostListener {

        private final List<MapPartitionLostEvent> events = new ArrayList<MapPartitionLostEvent>();

        private final int backupCount;

        public EventCollectingMapPartitionLostListener(int backupCount) {
            this.backupCount = backupCount;
        }

        @Override
        public synchronized void partitionLost(MapPartitionLostEvent event) {
            this.events.add(event);
        }

        public synchronized List<MapPartitionLostEvent> getEvents() {
            return new ArrayList<MapPartitionLostEvent>(events);
        }

        public int getBackupCount() {
            return backupCount;
        }
    }

    protected int getNodeCount() {
        return 5;
    }

    protected int getMapEntryCount() {
        return 5000;
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when1NodeCrashed_withoutData()
            throws InterruptedException {
        testMapPartitionLostListener(1, false);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when1NodeCrashed_withData()
            throws InterruptedException {
        testMapPartitionLostListener(1, true);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when2NodesCrashed_withoutData()
            throws InterruptedException {
        testMapPartitionLostListener(2, false);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when2NodesCrashed_withData()
            throws InterruptedException {
        testMapPartitionLostListener(2, true);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when3NodesCrashed_withoutData()
            throws InterruptedException {
        testMapPartitionLostListener(3, false);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when3NodesCrashed_withData()
            throws InterruptedException {
        testMapPartitionLostListener(3, true);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when4NodesCrashed_withoutData()
            throws InterruptedException {
        testMapPartitionLostListener(4, false);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when4NodesCrashed_withData()
            throws InterruptedException {
        testMapPartitionLostListener(4, true);
    }

    private void testMapPartitionLostListener(int numberOfNodesToCrash, boolean withData)
            throws InterruptedException {
        List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp();

        List<HazelcastInstance> survivingInstances = new ArrayList<HazelcastInstance>(instances);
        List<HazelcastInstance> terminatingInstances = survivingInstances.subList(0, numberOfNodesToCrash);
        survivingInstances = survivingInstances.subList(numberOfNodesToCrash, instances.size());

        List<EventCollectingMapPartitionLostListener> listeners = registerListeners(survivingInstances.get(0));

        if (withData) {
            populateMaps(survivingInstances.get(0));
        }

        String log = "Surviving: " + survivingInstances + " Terminating: " + terminatingInstances;
        Map<Integer, Integer> survivingPartitions = getMinReplicaIndicesByPartitionId(survivingInstances);

        terminateInstances(terminatingInstances);
        waitAllForSafeStateAndDumpPartitionServiceOnFailure(survivingInstances, 300);

        for (int i = 0; i < getNodeCount(); i++) {
            assertListenerInvocationsEventually(log, i, numberOfNodesToCrash, listeners.get(i), survivingPartitions);
        }
    }

    private void assertListenerInvocationsEventually(final String log, final int index, final int numberOfNodesToCrash,
                                                     final EventCollectingMapPartitionLostListener listener,
                                                     final Map<Integer, Integer> survivingPartitions) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                if (index < numberOfNodesToCrash) {
                    assertLostPartitions(log, listener, survivingPartitions);
                } else {
                    String message = log + " listener-" + index + " should not be invoked!";
                    assertTrue(message, listener.getEvents().isEmpty());
                }
            }
        });
    }

    private List<EventCollectingMapPartitionLostListener> registerListeners(HazelcastInstance instance) {
        List<EventCollectingMapPartitionLostListener> listeners = new ArrayList<EventCollectingMapPartitionLostListener>();
        for (int i = 0; i < getNodeCount(); i++) {
            EventCollectingMapPartitionLostListener listener = new EventCollectingMapPartitionLostListener(i);
            instance.getMap(getIthMapName(i)).addPartitionLostListener(listener);
            listeners.add(listener);
        }
        return listeners;
    }

    private void assertLostPartitions(String log, EventCollectingMapPartitionLostListener listener,
                                      Map<Integer, Integer> survivingPartitions) {
        List<MapPartitionLostEvent> events = listener.getEvents();
        assertFalse(survivingPartitions.isEmpty());

        for (MapPartitionLostEvent event : events) {
            int failedPartitionId = event.getPartitionId();
            Integer survivingReplicaIndex = survivingPartitions.get(failedPartitionId);
            if (survivingReplicaIndex != null) {
                String message = log + ", PartitionId: " + failedPartitionId + " SurvivingReplicaIndex: " + survivingReplicaIndex
                        + " Event: " + event.toString();
                assertTrue(message, survivingReplicaIndex > listener.getBackupCount());
            }
        }
    }

    /*
        This test works as follows:
        It starts a second node and kills it just after a partition is migrated to it.
        It also disables anti-entropy to prevent the first node sync its backup from the second node.
     */
    @Test
    public void testPartitionLostListenerOnNodeJoin() throws Exception {
        final Config config1 = new Config();
        config1.setProperty(PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "0");
        final HazelcastInstance instance1 = hazelcastInstanceFactory.newHazelcastInstance(config1);

        final IMap<Integer, Integer> map = instance1.getMap(mapName);
        final EventCollectingMapPartitionLostListener listener = new EventCollectingMapPartitionLostListener(1);
        map.addPartitionLostListener(listener);

        for (int i = 0; i < getMapEntryCount(); i++) {
            map.put(i, i);
        }

        final AtomicInteger migratedPartitionId = new AtomicInteger(-1);
        final MigrationListener migrationListener = new MigrationListener() {
            @Override
            public void migrationStarted(MigrationEvent migrationEvent) {

            }

            @Override
            public void migrationCompleted(MigrationEvent migrationEvent) {
                if (migrationEvent.getOldOwner().getAddress().equals(getAddress(instance1))) {
                    migratedPartitionId.compareAndSet(-1, migrationEvent.getPartitionId());
                }
            }

            @Override
            public void migrationFailed(MigrationEvent migrationEvent) {

            }
        };

        instance1.getPartitionService().addMigrationListener(migrationListener);

        final Thread thread2 = new Thread(new Runnable() {

            @Override
            public void run() {
                final Config config2 = new Config();
                config2.setProperty(PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "0");
                final HazelcastInstance instance2 = hazelcastInstanceFactory.newHazelcastInstance(config2);

                assertTrueEventually(new AssertTask() {
                    @Override
                    public void run()
                            throws Exception {
                        final int partitionId = migratedPartitionId.get();
                        assertNotEquals(-1, partitionId);
                        final long[] versions = TestPartitionUtils.getReplicaVersions(instance1, partitionId);
                        assertEquals(-1, versions[0]);
                    }
                });

                instance2.getLifecycleService().terminate();
            }
        });

        thread2.start();
        thread2.join();

        assertClusterSizeEventually(1, instance1);

        assertTrue(map.size() < getMapEntryCount());
        assertFalse(listener.getEvents().isEmpty());
    }

}
