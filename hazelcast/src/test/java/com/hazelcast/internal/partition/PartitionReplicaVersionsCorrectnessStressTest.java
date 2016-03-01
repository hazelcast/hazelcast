/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.AbstractPartitionLostListenerTest;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.TestPartitionUtils;
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

import static com.hazelcast.test.TestPartitionUtils.getReplicaAddresses;
import static com.hazelcast.test.TestPartitionUtils.getReplicaVersions;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(SlowTest.class)
public class PartitionReplicaVersionsCorrectnessStressTest extends AbstractPartitionLostListenerTest {

    private static final int ITEM_COUNT_PER_MAP = 10000;

    @Parameterized.Parameters(name = "numberOfNodesToCrash:{0},nodeCount:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{{1, 7}, {3, 7}, {6, 7}, {1, 10}, {3, 10}, {6, 10}});
    }

    @Override
    public int getNodeCount() {
        return nodeCount;
    }

    protected int getMapEntryCount() {
        return ITEM_COUNT_PER_MAP;
    }

    @Parameterized.Parameter(0)
    public int numberOfNodesToCrash;

    @Parameterized.Parameter(1)
    public int nodeCount;

    @Test
    public void testReplicaVersionsWhenNodesCrashSimultaneously() throws InterruptedException {
        List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp();

        List<HazelcastInstance> instancesCopy = new ArrayList<HazelcastInstance>(instances);
        List<HazelcastInstance> terminatingInstances = instancesCopy.subList(0, numberOfNodesToCrash);
        List<HazelcastInstance> survivingInstances = instancesCopy.subList(numberOfNodesToCrash, instances.size());
        populateMaps(survivingInstances.get(0));

        String log = "Surviving: " + survivingInstances + " Terminating: " + terminatingInstances;

        Map<Integer, long[]> replicaVersionsByPartitionId = TestPartitionUtils.getAllReplicaVersions(instances);
        Map<Integer, List<Address>> partitionReplicaAddresses = TestPartitionUtils.getAllReplicaAddresses(instances);
        Map<Integer, Integer> minSurvivingReplicaIndexByPartitionId = getMinReplicaIndicesByPartitionId(survivingInstances);

        terminateInstances(terminatingInstances);
        waitAllForSafeStateAndDumpPartitionServiceOnFailure(survivingInstances, 300);

        validateReplicaVersions(log, numberOfNodesToCrash, survivingInstances, replicaVersionsByPartitionId,
                partitionReplicaAddresses, minSurvivingReplicaIndexByPartitionId);
    }

    private void validateReplicaVersions(String log, int numberOfNodesToCrash,
                                         List<HazelcastInstance> survivingInstances,
                                         Map<Integer, long[]> replicaVersionsByPartitionId,
                                         Map<Integer, List<Address>> partitionReplicaAddresses,
                                         Map<Integer, Integer> minSurvivingReplicaIndexByPartitionId)
            throws InterruptedException {
        for (HazelcastInstance instance : survivingInstances) {
            Node node = getNode(instance);
            Address address = node.getThisAddress();

            InternalPartitionService partitionService = node.getPartitionService();
            for (InternalPartition partition : partitionService.getInternalPartitions()) {
                if (address.equals(partition.getOwnerOrNull())) {
                    int partitionId = partition.getPartitionId();
                    long[] initialReplicaVersions = replicaVersionsByPartitionId.get(partitionId);
                    Integer minSurvivingReplicaIndex = minSurvivingReplicaIndexByPartitionId.get(partitionId);
                    long[] replicaVersions = getReplicaVersions(instance, partitionId);
                    List<Address> addresses = getReplicaAddresses(instance, partitionId);

                    String message = log + " PartitionId: " + partitionId
                            + " InitialReplicaVersions: " + Arrays.toString(initialReplicaVersions)
                            + " ReplicaVersions: " + Arrays.toString(replicaVersions)
                            + " SmallestSurvivingReplicaIndex: " + minSurvivingReplicaIndex
                            + " InitialReplicaAddresses: " + partitionReplicaAddresses.get(partitionId)
                            + " Instance: " + address + " CurrentReplicaAddresses: " + addresses;

                    if (minSurvivingReplicaIndex <= 1) {
                        assertArrayEquals(message, initialReplicaVersions, replicaVersions);
                    } else if (numberOfNodesToCrash > 1) {
                        final long[] expected = Arrays.copyOf(initialReplicaVersions, initialReplicaVersions.length);

                        boolean verified;
                        int i = 1;
                        do {
                            verified = Arrays.equals(expected, replicaVersions);
                            shiftLeft(expected, i, replicaVersions[i - 1]);
                        } while (i++ <= minSurvivingReplicaIndex && !verified);

                        if (!verified) {
                            fail(message);
                        }
                    } else {
                        fail(message);
                    }
                }
            }
        }
    }

    private void shiftLeft(final long[] versions, final int toIndex, final long version) {
        Arrays.fill(versions, 0, toIndex, version);
    }

}
