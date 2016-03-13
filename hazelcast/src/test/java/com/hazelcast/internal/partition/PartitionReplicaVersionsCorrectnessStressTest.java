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
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.partition.AbstractPartitionLostListenerTest;
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
public class PartitionReplicaVersionsCorrectnessStressTest extends AbstractPartitionLostListenerTest {

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
    public void testReplicaVersions_when1NodeCrashes() throws InterruptedException {
        testReplicaVersionsWhenNodesCrashSimultaneously(1);
    }

    @Test
    public void testReplicaVersions_when2NodesCrashSimultaneously() throws InterruptedException {
        testReplicaVersionsWhenNodesCrashSimultaneously(2);
    }

    @Test
    public void testReplicaVersions_when3NodesCrashSimultaneously() throws InterruptedException {
        testReplicaVersionsWhenNodesCrashSimultaneously(3);
    }

    @Test
    public void testReplicaVersions_when4NodesCrashSimultaneously() throws InterruptedException {
        testReplicaVersionsWhenNodesCrashSimultaneously(4);
    }

    private void testReplicaVersionsWhenNodesCrashSimultaneously(int numberOfNodesToCrash) throws InterruptedException {
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
            for (InternalPartition partition : partitionService.getPartitions()) {
                if (address.equals(partition.getOwnerOrNull())) {
                    int partitionId = partition.getPartitionId();
                    long[] initialReplicaVersions = replicaVersionsByPartitionId.get(partitionId);
                    Integer minSurvivingReplicaIndex = minSurvivingReplicaIndexByPartitionId.get(partitionId);
                    long[] replicaVersions = TestPartitionUtils.getReplicaVersions(instance, partitionId);
                    List<Address> addresses = TestPartitionUtils.getReplicaAddresses(instance, partitionId);

                    String message = log + " PartitionId: " + partitionId
                            + " InitialReplicaVersions: " + Arrays.toString(initialReplicaVersions)
                            + " ReplicaVersions: " + Arrays.toString(replicaVersions)
                            + " SmallestSurvivingReplicaIndex: " + minSurvivingReplicaIndex
                            + " InitialReplicaAddresses: " + partitionReplicaAddresses.get(partitionId)
                            + " Instance: " + address + " CurrentReplicaAddresses: " + addresses;

                    if (minSurvivingReplicaIndex <= 1) {
                        assertArrayEquals(message, initialReplicaVersions, replicaVersions);
                    } else if (numberOfNodesToCrash > 1) {
                        for (int i = minSurvivingReplicaIndex; i < replicaVersions.length; i++) {
                            assertEquals(message, initialReplicaVersions[i], replicaVersions[i]);
                        }

                        long duplicatedReplicaVersion = initialReplicaVersions[minSurvivingReplicaIndex - 1];
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
