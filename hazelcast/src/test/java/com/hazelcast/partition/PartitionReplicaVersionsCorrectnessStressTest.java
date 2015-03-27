package com.hazelcast.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
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

import static java.lang.Math.min;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
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
    public void testReplicaVersions_when1NodeCrashes() {
        testReplicaVersionsWhenNodesCrashSimultaneously(1);
    }

    @Test
    public void testReplicaVersions_when2NodesCrashSimultaneously() {
        testReplicaVersionsWhenNodesCrashSimultaneously(2);
    }

    @Test
    public void testReplicaVersions_when3NodesCrashSimultaneously() {
        testReplicaVersionsWhenNodesCrashSimultaneously(3);
    }

    @Test
    public void testReplicaVersions_when4NodesCrashSimultaneously() {
        testReplicaVersionsWhenNodesCrashSimultaneously(4);
    }

    private void testReplicaVersionsWhenNodesCrashSimultaneously(final int numberOfNodesToCrash) {
        final List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp();

        List<HazelcastInstance> survivingInstances = new ArrayList<HazelcastInstance>(instances);
        final List<HazelcastInstance> terminatingInstances = survivingInstances.subList(0, numberOfNodesToCrash);
        survivingInstances = survivingInstances.subList(numberOfNodesToCrash, instances.size());

        populateMaps(survivingInstances.get(0));

        final String log = "Surviving: " + survivingInstances + " Terminating: " + terminatingInstances;

        final Map<Integer, long[]> replicaVersionsByPartitionId = new HashMap<Integer, long[]>();
        final Map<Integer, Integer> smallestSurvivingReplicaIndexByPartitionId = new HashMap<Integer, Integer>();
        collect(numberOfNodesToCrash, instances, replicaVersionsByPartitionId, smallestSurvivingReplicaIndexByPartitionId);

        terminateInstances(terminatingInstances);
        waitAllForSafeState(survivingInstances);

        validateReplicaVersions(numberOfNodesToCrash, log, survivingInstances, replicaVersionsByPartitionId,
                smallestSurvivingReplicaIndexByPartitionId);
    }

    private void collect(final int numberOfNodesToCrash,
                         final List<HazelcastInstance> instances,
                         final Map<Integer, long[]> replicaVersionsByPartitionId,
                         final Map<Integer, Integer> smallestSurvivingReplicaIndexByPartitionId) {
        for (int i = 0; i < instances.size(); i++) {
            final HazelcastInstance instance = instances.get(i);
            final Node node = getNode(instance);
            final Address address = node.getThisAddress();

            final InternalPartitionService partitionService = node.getPartitionService();
            for (InternalPartition partition : partitionService.getPartitions()) {
                final int partitionId = partition.getPartitionId();
                final long[] replicaVersions = partitionService.getPartitionReplicaVersions(partitionId);

                if (address.equals(partition.getOwnerOrNull())) {
                    final long[] replicaVersionsCopy = Arrays.copyOf(replicaVersions, replicaVersions.length);
                    replicaVersionsByPartitionId.put(partitionId, replicaVersionsCopy);
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

    private void validateReplicaVersions(final int numberOfNodesToCrash, final String log,
                                         final List<HazelcastInstance> survivingInstances,
                                         final Map<Integer, long[]> replicaVersionsByPartitionId,
                                         final Map<Integer, Integer> smallestSurvivingReplicaIndexByPartitionId) {
        for (HazelcastInstance instance : survivingInstances) {
            final Node node = getNode(instance);
            final Address address = node.getThisAddress();

            final InternalPartitionService partitionService = node.getPartitionService();
            for (InternalPartition partition : partitionService.getPartitions()) {
                if (address.equals(partition.getOwnerOrNull())) {
                    final int partitionId = partition.getPartitionId();
                    final long[] initialReplicaVersions = replicaVersionsByPartitionId.get(partitionId);
                    final Integer smallestSurvivingReplicaIndex = smallestSurvivingReplicaIndexByPartitionId.get(partitionId);
                    final long[] replicaVersions = partitionService.getPartitionReplicaVersions(partitionId);

                    final String message = log + " PartitionId: " + partitionId + " InitialReplicaVersions: " +
                            Arrays.toString(initialReplicaVersions) + " ReplicaVersions: " + Arrays.toString(replicaVersions)
                            + " SmallestSurvivingReplicaIndex: " + smallestSurvivingReplicaIndex;

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
