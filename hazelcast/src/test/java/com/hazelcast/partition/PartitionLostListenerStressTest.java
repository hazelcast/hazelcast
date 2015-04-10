package com.hazelcast.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class PartitionLostListenerStressTest
        extends AbstractPartitionLostListenerTest {

    public static class EventCollectingPartitionLostListener
            implements PartitionLostListener {

        private List<PartitionLostEvent> lostPartitions = new ArrayList<PartitionLostEvent>();

        @Override
        public synchronized void partitionLost(PartitionLostEvent event) {
            lostPartitions.add(event);
        }

        public synchronized List<PartitionLostEvent> getEvents() {
            return new ArrayList<PartitionLostEvent>(lostPartitions);
        }
    }

    protected int getNodeCount() {
        return 5;
    }

    protected int getMapEntryCount() {
        return 5000;
    }

    @Test
    public void test_partitionLostListenerInvoked_when1NodeCrashed_withoutData()
            throws InterruptedException {
        testPartitionLostListener(1, false);
    }

    @Test
    public void test_partitionLostListenerInvoked_when1NodeCrashed_withData()
            throws InterruptedException {
        testPartitionLostListener(1, true);
    }

    @Test
    public void test_partitionLostListenerInvoked_when2NodesCrashed_withoutData()
            throws InterruptedException {
        testPartitionLostListener(2, false);
    }

    @Test
    public void test_partitionLostListenerInvoked_when2NodesCrashed_withData()
            throws InterruptedException {
        testPartitionLostListener(2, true);
    }

    @Test
    public void test_partitionLostListenerInvoked_when3NodesCrashed_withoutData()
            throws InterruptedException {
        testPartitionLostListener(3, false);
    }

    @Test
    public void test_partitionLostListenerInvoked_when3NodesCrashed_withData()
            throws InterruptedException {
        testPartitionLostListener(3, true);
    }

    @Test
    public void test_partitionLostListenerInvoked_when4NodesCrashed_withoutData()
            throws InterruptedException {
        testPartitionLostListener(4, false);
    }

    @Test
    public void test_partitionLostListenerInvoked_when4NodesCrashed_withData()
            throws InterruptedException {
        testPartitionLostListener(4, true);
    }

    @Test
    public void test_partitionLostListenerNotInvoked_whenNewNodesJoin() {
        final HazelcastInstance master = createInstances(1).get(0);
        final EventCollectingPartitionLostListener listener = registerPartitionLostListener(master);
        final List<HazelcastInstance> others = createInstances(getNodeCount() - 1);

        waitAllForSafeState(master);
        waitAllForSafeState(others);

        assertTrue("No invocation to PartitionLostListener when new nodes join to cluster", listener.getEvents().isEmpty());
    }

    private void testPartitionLostListener(final int numberOfNodesToCrash, final boolean withData) {
        final List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp();

        List<HazelcastInstance> survivingInstances = new ArrayList<HazelcastInstance>(instances);
        final List<HazelcastInstance> terminatingInstances = survivingInstances.subList(0, numberOfNodesToCrash);
        survivingInstances = survivingInstances.subList(numberOfNodesToCrash, instances.size());

        if (withData) {
            populateMaps(survivingInstances.get(0));
        }

        final String log = "Surviving: " + survivingInstances + " Terminating: " + terminatingInstances;
        final EventCollectingPartitionLostListener listener = registerPartitionLostListener(survivingInstances.get(0));
        final Map<Integer, Integer> survivingPartitions = getMinReplicaIndicesByPartitionId(survivingInstances);

        terminateInstances(terminatingInstances);
        waitAllForSafeState(survivingInstances);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertLostPartitions(log, listener, survivingPartitions);
            }
        });
    }

    private void assertLostPartitions(final String log, final EventCollectingPartitionLostListener listener,
                                      final Map<Integer, Integer> survivingPartitions) {
        final List<PartitionLostEvent> failedPartitions = listener.getEvents();

        assertFalse(survivingPartitions.isEmpty());

        Map<Integer, Integer> memorizedPartitionFailures = new HashMap<Integer, Integer>();

        for (PartitionLostEvent event : failedPartitions) {
            final int failedPartitionId = event.getPartitionId();
            final int lostReplicaIndex = event.getLostBackupCount();
            final int survivingReplicaIndex = survivingPartitions.get(failedPartitionId);

            final String message = log + ", PartitionId: " + failedPartitionId + " LostReplicaIndex: " + lostReplicaIndex
                    + " SurvivingReplicaIndex: " + survivingReplicaIndex + " Event Source: " + event.getEventSource();

            assertTrue(message, survivingReplicaIndex > 0);
            assertTrue(message, lostReplicaIndex >= 0);
            assertTrue(message, lostReplicaIndex <= survivingReplicaIndex - 1);

            final Integer previouslyLostReplicaIndex = memorizedPartitionFailures.get(failedPartitionId);
            if (previouslyLostReplicaIndex != null) {
                assertTrue(message + " PreviouslyLostReplicaIndex: " + previouslyLostReplicaIndex,
                        previouslyLostReplicaIndex < lostReplicaIndex);
            }

            memorizedPartitionFailures.put(failedPartitionId, lostReplicaIndex);
        }
    }

    private EventCollectingPartitionLostListener registerPartitionLostListener(final HazelcastInstance instance) {
        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();
        instance.getPartitionService().addPartitionLostListener(listener);
        return listener;
    }

}
