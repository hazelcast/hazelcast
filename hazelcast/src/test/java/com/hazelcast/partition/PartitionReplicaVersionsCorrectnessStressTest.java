package com.hazelcast.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.min;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
//@Repeat(100)
public class PartitionReplicaVersionsCorrectnessStressTest
        extends AbstractPartitionLostListenerTest {

    private static final int NODE_COUNT = 5;

    private static final int ITEM_COUNT_PER_MAP = 10000;

    @Override
    public int getNodeCount() {
        return NODE_COUNT;
    }

    protected int getMapEntryCount() {
        return ITEM_COUNT_PER_MAP;
    }

    @Test
    public void testReplicaVersions_when1NodeCrashes()
            throws InterruptedException {
        testReplicaVersionsWhenNodesCrashSimultaneously(1);
    }

    @Test
    public void testReplicaVersions_when2NodesCrashSimultaneously()
            throws InterruptedException {
        testReplicaVersionsWhenNodesCrashSimultaneously(2);
    }

    @Test
    public void testReplicaVersions_when3NodesCrashSimultaneously()
            throws InterruptedException {
        testReplicaVersionsWhenNodesCrashSimultaneously(3);
    }

    @Test
    public void testReplicaVersions_when4NodesCrashSimultaneously()
            throws InterruptedException {
        testReplicaVersionsWhenNodesCrashSimultaneously(4);
    }

    private void testReplicaVersionsWhenNodesCrashSimultaneously(final int numberOfNodesToCrash)
            throws InterruptedException {
        final List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp();

        List<HazelcastInstance> instancesCopy = new ArrayList<HazelcastInstance>(instances);
        final List<HazelcastInstance> terminatingInstances = instancesCopy.subList(0, numberOfNodesToCrash);
        final List<HazelcastInstance> survivingInstances = instancesCopy.subList(numberOfNodesToCrash, instances.size());
        populateMaps(survivingInstances.get(0));

        final String log = "Surviving: " + survivingInstances + " Terminating: " + terminatingInstances;

        final Map<Integer, long[]> replicaVersionsByPartitionId = new HashMap<Integer, long[]>();
        final Map<Integer, List<Address>> partitionReplicaAddresses = new HashMap<Integer, List<Address>>();
        final Map<Integer, Integer> smallestSurvivingReplicaIndexByPartitionId = new HashMap<Integer, Integer>();

        collect(numberOfNodesToCrash, instances, replicaVersionsByPartitionId, partitionReplicaAddresses,
                smallestSurvivingReplicaIndexByPartitionId);

        terminateInstances(terminatingInstances);
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertTrue(isAllInSafeState(survivingInstances));
            }
        }, 300);

        validateReplicaVersions(numberOfNodesToCrash, log, survivingInstances, replicaVersionsByPartitionId,
                partitionReplicaAddresses, smallestSurvivingReplicaIndexByPartitionId);
    }

    private void collect(final int numberOfNodesToCrash, final List<HazelcastInstance> instances,
                         final Map<Integer, long[]> replicaVersionsByPartitionId,
                         final Map<Integer, List<Address>> partitionReplicaAddresses,
                         final Map<Integer, Integer> smallestSurvivingReplicaIndexByPartitionId)
            throws InterruptedException {
        for (int i = 0; i < instances.size(); i++) {
            final HazelcastInstance instance = instances.get(i);
            final Node node = getNode(instance);
            final Address address = node.getThisAddress();

            final InternalPartitionService partitionService = node.getPartitionService();
            for (InternalPartition partition : partitionService.getPartitions()) {
                final int partitionId = partition.getPartitionId();

                final long[] replicaVersions = getReplicaVersions(node, partitionId);

                if (address.equals(partition.getOwnerOrNull())) {
                    final long[] replicaVersionsCopy = Arrays.copyOf(replicaVersions, replicaVersions.length);
                    replicaVersionsByPartitionId.put(partitionId, replicaVersionsCopy);

                    final List<Address> addresses = getAddresses(i, partition);
                    partitionReplicaAddresses.put(partitionId, addresses);

                }

                if (i >= numberOfNodesToCrash) { // instance is surviving
                    for (int j = 0; j < replicaVersions.length; j++) {
                        if (address.equals(partition.getReplicaAddress(j))) {
                            Integer smallestSurvivingReplicaIndex = smallestSurvivingReplicaIndexByPartitionId.get(partitionId);
                            smallestSurvivingReplicaIndex =
                                    smallestSurvivingReplicaIndex != null ? min(smallestSurvivingReplicaIndex, j) : j;
                            smallestSurvivingReplicaIndexByPartitionId.put(partitionId, smallestSurvivingReplicaIndex);
                        }
                    }
                }
            }
        }
    }

    // Must be called on the correct node !!!
    private long[] getReplicaVersions(Node node, int partitionId)
            throws InterruptedException {
        final GetReplicaVersionsRunnable runnable = new GetReplicaVersionsRunnable(node, partitionId);
        node.getNodeEngine().getOperationService().execute(runnable);
        return runnable.getReplicaVersions();
    }

    private static class GetReplicaVersionsRunnable
            implements PartitionSpecificRunnable {

        private final Node node;

        private final int partitionId;

        private final CountDownLatch latch = new CountDownLatch(1);

        private long[] replicaVersions;

        public GetReplicaVersionsRunnable(Node node, int partitionId) {
            this.node = node;
            this.partitionId = partitionId;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            final InternalPartitionService partitionService = node.nodeEngine.getPartitionService();
            final long[] replicaVersions = partitionService.getPartitionReplicaVersions(getPartitionId());
            this.replicaVersions = Arrays.copyOf(replicaVersions, replicaVersions.length);
            latch.countDown();
        }

        public long[] getReplicaVersions()
                throws InterruptedException {
            latch.await(30, TimeUnit.SECONDS);
            return replicaVersions;
        }
    }

    private List<Address> getAddresses(int i, InternalPartition partition) {
        final List<Address> addresses = new ArrayList<Address>();
        for (int j = 0; j < i; j++) {
            addresses.add(partition.getReplicaAddress(j));
        }
        return addresses;
    }

    private void validateReplicaVersions(final int numberOfNodesToCrash, final String log,
                                         final List<HazelcastInstance> survivingInstances,
                                         final Map<Integer, long[]> replicaVersionsByPartitionId,
                                         final Map<Integer, List<Address>> partitionReplicaAddresses,
                                         final Map<Integer, Integer> smallestSurvivingReplicaIndexByPartitionId)
            throws InterruptedException {
        for (HazelcastInstance instance : survivingInstances) {
            final Node node = getNode(instance);
            final Address address = node.getThisAddress();

            final InternalPartitionService partitionService = node.getPartitionService();
            for (InternalPartition partition : partitionService.getPartitions()) {
                if (address.equals(partition.getOwnerOrNull())) {
                    final int partitionId = partition.getPartitionId();
                    final long[] initialReplicaVersions = replicaVersionsByPartitionId.get(partitionId);
                    final Integer smallestSurvivingReplicaIndex = smallestSurvivingReplicaIndexByPartitionId.get(partitionId);
                    final long[] replicaVersions = getReplicaVersions(node, partitionId);
                    final List<Address> addresses = getAddresses(survivingInstances.size(), partition);

                    final String message = log + " PartitionId: " + partitionId + " InitialReplicaVersions: " +
                            Arrays.toString(initialReplicaVersions) + " ReplicaVersions: " + Arrays.toString(replicaVersions)
                            + " SmallestSurvivingReplicaIndex: " + smallestSurvivingReplicaIndex + " InitialReplicaAddresses: "
                            + partitionReplicaAddresses.get(partitionId) + " Instance: " + address + " CurrentReplicaAddresses: "
                            + addresses;

                    if (smallestSurvivingReplicaIndex <= 1) {
                        assertArrayEquals(message, initialReplicaVersions, replicaVersions);
                    } else if (numberOfNodesToCrash > 1) {
                        for (int i = smallestSurvivingReplicaIndex; i < replicaVersions.length; i++) {
                            assertEquals(message, initialReplicaVersions[i], replicaVersions[i]);
                        }

                        final long duplicatedReplicaVersion = initialReplicaVersions[smallestSurvivingReplicaIndex - 1];
                        for (int i = 0; i < smallestSurvivingReplicaIndex; i++) {
                            assertEquals(duplicatedReplicaVersion, replicaVersions[i]);
                        }
                    } else {
                        fail(message);
                    }
                }
            }
        }
    }

}
