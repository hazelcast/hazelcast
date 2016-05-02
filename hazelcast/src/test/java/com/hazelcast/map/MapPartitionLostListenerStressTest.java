package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.partition.AbstractPartitionLostListenerTest;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
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

    @Parameterized.Parameters(name = "numberOfNodesToCrash:{0},withData:{1},nodeLeaveType:{2},shouldExpectPartitionLostEvents:{3}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {1, true, NodeLeaveType.SHUTDOWN, false},
                {1, true, NodeLeaveType.TERMINATE, true},
                {1, false, NodeLeaveType.SHUTDOWN, false},
                {1, false, NodeLeaveType.TERMINATE, true},
                {2, true, NodeLeaveType.SHUTDOWN, false},
                {2, true, NodeLeaveType.TERMINATE, true},
                {2, false, NodeLeaveType.SHUTDOWN, false},
                {2, false, NodeLeaveType.TERMINATE, true},
                {3, true, NodeLeaveType.SHUTDOWN, false},
                {3, true, NodeLeaveType.TERMINATE, true},
                {3, false, NodeLeaveType.SHUTDOWN, false},
                {3, false, NodeLeaveType.TERMINATE, true}
        });
    }

    @Parameterized.Parameter(0)
    public int numberOfNodesToCrash;

    @Parameterized.Parameter(1)
    public boolean withData;

    @Parameterized.Parameter(2)
    public NodeLeaveType nodeLeaveType;

    @Parameterized.Parameter(3)
    public boolean shouldExpectPartitionLostEvents;

    protected int getNodeCount() {
        return 5;
    }

    protected int getMapEntryCount() {
        return 5000;
    }

    @Test
    public void testMapPartitionLostListener()
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

        stopInstances(terminatingInstances, nodeLeaveType);
        waitAllForSafeStateAndDumpPartitionServiceOnFailure(survivingInstances, 300);

        if (shouldExpectPartitionLostEvents) {
            for (int i = 0; i < getNodeCount(); i++) {
                assertListenerInvocationsEventually(log, i, numberOfNodesToCrash, listeners.get(i), survivingPartitions);
            }
        } else {
            for (final EventCollectingMapPartitionLostListener listener : listeners) {
                assertTrueAllTheTime(new AssertTask() {
                    @Override
                    public void run()
                            throws Exception {
                        assertTrue(listener.getEvents().isEmpty());
                    }
                }, 1);
            }
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
}
