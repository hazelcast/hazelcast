package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.partition.AbstractPartitionLostListenerTest;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapPartitionLostListenerStressTest extends AbstractPartitionLostListenerTest {

    public static class EventCollectingMapPartitionLostListener implements MapPartitionLostListener {

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
    public void test_mapPartitionLostListenerInvoked_when1NodeCrashed_withoutData() throws InterruptedException {
        testMapPartitionLostListener(1, false);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when1NodeCrashed_withData() throws InterruptedException {
        testMapPartitionLostListener(1, true);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when2NodesCrashed_withoutData() throws InterruptedException {
        testMapPartitionLostListener(2, false);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when2NodesCrashed_withData() throws InterruptedException {
        testMapPartitionLostListener(2, true);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when3NodesCrashed_withoutData() throws InterruptedException {
        testMapPartitionLostListener(3, false);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when3NodesCrashed_withData() throws InterruptedException {
        testMapPartitionLostListener(3, true);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when4NodesCrashed_withoutData() throws InterruptedException {
        testMapPartitionLostListener(4, false);
    }

    @Test
    public void test_mapPartitionLostListenerInvoked_when4NodesCrashed_withData() throws InterruptedException {
        testMapPartitionLostListener(4, true);
    }

    private void testMapPartitionLostListener(int numberOfNodesToCrash, boolean withData) throws InterruptedException {
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
                String message = log + ", PartitionId: " + failedPartitionId
                        + " SurvivingReplicaIndex: " + survivingReplicaIndex + " Event: " + event.toString();
                assertTrue(message, survivingReplicaIndex > listener.getBackupCount());
            }
        }
    }
}
