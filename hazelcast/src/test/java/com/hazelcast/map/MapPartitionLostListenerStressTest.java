package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.AbstractPartitionLostListenerTest;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapPartitionLostListenerStressTest
        extends AbstractPartitionLostListenerTest {

    public static class EventCollectingMapPartitionLostListener
            implements MapPartitionLostListener {

        private final List<MapPartitionLostEvent> events = Collections.synchronizedList(new LinkedList<MapPartitionLostEvent>());

        private final int backupCount;

        public EventCollectingMapPartitionLostListener(int backupCount) {
            this.backupCount = backupCount;
        }

        @Override
        public void partitionLost(MapPartitionLostEvent event) {
            this.events.add(event);
        }

        public List<MapPartitionLostEvent> getEvents() {
            synchronized (events) {
                return new ArrayList<MapPartitionLostEvent>(events);
            }
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

    private void testMapPartitionLostListener(final int numberOfNodesToCrash, final boolean withData) {
        final List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp();

        List<HazelcastInstance> survivingInstances = new ArrayList<HazelcastInstance>(instances);
        final List<HazelcastInstance> terminatingInstances = survivingInstances.subList(0, numberOfNodesToCrash);
        survivingInstances = survivingInstances.subList(numberOfNodesToCrash, instances.size());

        final List<EventCollectingMapPartitionLostListener> listeners = registerListeners(survivingInstances.get(0));

        if (withData) {
            populateMaps(survivingInstances.get(0));
        }

        final String log = "Surviving: " + survivingInstances + " Terminating: " + terminatingInstances;
        final Map<Integer, Integer> survivingPartitions = getMinReplicaIndicesByPartitionId(survivingInstances);

        terminateInstances(terminatingInstances);
        waitAllForSafeState(survivingInstances);

        for (int i = 0; i < getNodeCount(); i++) {
            assertListenerInvocationsEventually(numberOfNodesToCrash, log, survivingPartitions, listeners.get(i), i);
        }
    }

    private void assertListenerInvocationsEventually(final int numberOfNodesToCrash, final String log,
                                                     final Map<Integer, Integer> survivingPartitions,
                                                     final EventCollectingMapPartitionLostListener listener, final int index) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                if (index < numberOfNodesToCrash) {
                    assertLostPartitions(log, listener, survivingPartitions);
                } else {
                    final String message = log + " listener-" + index + " should not be invoked!";
                    assertTrue(message, listener.getEvents().isEmpty());
                }
            }
        });
    }

    private void assertLostPartitions(final String log, final EventCollectingMapPartitionLostListener listener,
                                      final Map<Integer, Integer> survivingPartitions) {
        final List<MapPartitionLostEvent> events = listener.getEvents();

        assertFalse(survivingPartitions.isEmpty());

        for (MapPartitionLostEvent event : events) {
            final int failedPartitionId = event.getPartitionId();
            final Integer survivingReplicaIndex = survivingPartitions.get(failedPartitionId);
            if (survivingReplicaIndex != null) {
                final String message =
                        log + ", PartitionId: " + failedPartitionId + " SurvivingReplicaIndex: " + survivingReplicaIndex
                                + " Map Name: " + event.getName();
                assertTrue(message, survivingReplicaIndex > listener.getBackupCount());
            }
        }
    }

    private List<EventCollectingMapPartitionLostListener> registerListeners(final HazelcastInstance instance) {
        final List<EventCollectingMapPartitionLostListener> listeners = new ArrayList<EventCollectingMapPartitionLostListener>();
        for (int i = 0; i < getNodeCount(); i++) {
            final EventCollectingMapPartitionLostListener listener = new EventCollectingMapPartitionLostListener(i);
            instance.getMap(getIthMapName(i)).addPartitionLostListener(listener);
            listeners.add(listener);
        }

        return listeners;
    }

    private Map<Integer, Integer> getMinReplicaIndicesByPartitionId(final List<HazelcastInstance> instances) {
        final Map<Integer, Integer> survivingPartitions = new HashMap<Integer, Integer>();

        for (HazelcastInstance instance : instances) {
            final Node survivingNode = getNode(instance);
            final Address survivingNodeAddress = survivingNode.getThisAddress();

            for (InternalPartition partition : survivingNode.getPartitionService().getPartitions()) {
                if (partition.isOwnerOrBackup(survivingNodeAddress)) {
                    for (int replicaIndex = 0; replicaIndex < getNodeCount(); replicaIndex++) {
                        if (survivingNodeAddress.equals(partition.getReplicaAddress(replicaIndex))) {
                            final Integer replicaIndexOfOtherInstance = survivingPartitions.get(partition.getPartitionId());
                            if (replicaIndexOfOtherInstance != null) {
                                survivingPartitions
                                        .put(partition.getPartitionId(), Math.min(replicaIndex, replicaIndexOfOtherInstance));
                            } else {
                                survivingPartitions.put(partition.getPartitionId(), replicaIndex);
                            }

                            break;
                        }
                    }
                }
            }
        }

        return survivingPartitions;
    }

}
