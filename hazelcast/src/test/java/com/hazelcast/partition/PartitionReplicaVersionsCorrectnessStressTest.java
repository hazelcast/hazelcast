package com.hazelcast.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestPartitionUtils;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

        final Map<Integer, long[]> replicaVersionsByPartitionId = TestPartitionUtils.getAllReplicaVersions(instances);
        final Map<Integer, List<Address>> partitionReplicaAddresses = TestPartitionUtils.getAllReplicaAddresses(instances);
        final Map<Integer, Integer> minSurvivingReplicaIndexByPartitionId = getMinReplicaIndicesByPartitionId(survivingInstances);

        terminateInstances(terminatingInstances);
        waitAllForSafeStateAndDumpPartitionServiceOnFailure(survivingInstances, 300);

        validateReplicaVersions(numberOfNodesToCrash, log, survivingInstances, replicaVersionsByPartitionId,
                partitionReplicaAddresses, minSurvivingReplicaIndexByPartitionId);
    }

    private void validateReplicaVersions(final int numberOfNodesToCrash, final String log,
                                         final List<HazelcastInstance> survivingInstances,
                                         final Map<Integer, long[]> replicaVersionsByPartitionId,
                                         final Map<Integer, List<Address>> partitionReplicaAddresses,
                                         final Map<Integer, Integer> minSurvivingReplicaIndexByPartitionId)
            throws InterruptedException {
        for (HazelcastInstance instance : survivingInstances) {
            final Node node = getNode(instance);
            final Address address = node.getThisAddress();

            final InternalPartitionService partitionService = node.getPartitionService();
            for (InternalPartition partition : partitionService.getPartitions()) {
                if (address.equals(partition.getOwnerOrNull())) {
                    final int partitionId = partition.getPartitionId();
                    final long[] initialReplicaVersions = replicaVersionsByPartitionId.get(partitionId);
                    final Integer minSurvivingReplicaIndex = minSurvivingReplicaIndexByPartitionId.get(partitionId);
                    final long[] replicaVersions = TestPartitionUtils.getReplicaVersions(instance, partitionId);
                    final List<Address> addresses = TestPartitionUtils.getReplicaAddresses(instance, partitionId);

                    final String message = log + " PartitionId: " + partitionId + " InitialReplicaVersions: " +
                            Arrays.toString(initialReplicaVersions) + " ReplicaVersions: " + Arrays.toString(replicaVersions)
                            + " SmallestSurvivingReplicaIndex: " + minSurvivingReplicaIndex + " InitialReplicaAddresses: "
                            + partitionReplicaAddresses.get(partitionId) + " Instance: " + address + " CurrentReplicaAddresses: "
                            + addresses;

                    if (minSurvivingReplicaIndex <= 1) {
                        assertArrayEquals(message, initialReplicaVersions, replicaVersions);
                    } else if (numberOfNodesToCrash > 1) {
                        for (int i = minSurvivingReplicaIndex; i < replicaVersions.length; i++) {
                            assertEquals(message, initialReplicaVersions[i], replicaVersions[i]);
                        }

                        final long duplicatedReplicaVersion = initialReplicaVersions[minSurvivingReplicaIndex - 1];
                        for (int i = 0; i < minSurvivingReplicaIndex; i++) {
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
