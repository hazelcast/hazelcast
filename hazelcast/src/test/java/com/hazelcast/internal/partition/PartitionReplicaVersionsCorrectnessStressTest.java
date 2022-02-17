/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.partition.AbstractPartitionLostListenerTest;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.partition.TestPartitionUtils.getPartitionReplicaVersionsView;
import static com.hazelcast.internal.partition.TestPartitionUtils.getReplicaAddresses;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class PartitionReplicaVersionsCorrectnessStressTest extends AbstractPartitionLostListenerTest {

    private static final int ITEM_COUNT_PER_MAP = 10000;
    private static final int SAFE_STATE_TIMEOUT_SECONDS = 300;
    private static final long TEST_TIMEOUT_SECONDS = SAFE_STATE_TIMEOUT_SECONDS + 120;

    @Parameterized.Parameters(name = "numberOfNodesToCrash:{0},nodeCount:{1},nodeLeaveType:{2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {1, 5, NodeLeaveType.SHUTDOWN},
                {1, 5, NodeLeaveType.TERMINATE},
                {3, 7, NodeLeaveType.SHUTDOWN},
                {3, 7, NodeLeaveType.TERMINATE},
                {6, 7, NodeLeaveType.SHUTDOWN},
                {6, 7, NodeLeaveType.TERMINATE},
        });
    }

    @Override
    public int getNodeCount() {
        return nodeCount;
    }

    protected int getMapEntryCount() {
        return ITEM_COUNT_PER_MAP;
    }

    @Parameterized.Parameter(0)
    public int numberOfNodesToStop;

    @Parameterized.Parameter(1)
    public int nodeCount;

    @Parameterized.Parameter(2)
    public NodeLeaveType nodeLeaveType;

    @Test(timeout = TEST_TIMEOUT_SECONDS * 1000)
    public void testReplicaVersionsWhenNodesCrashSimultaneously() throws InterruptedException {
        List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp();

        List<HazelcastInstance> instancesCopy = new ArrayList<HazelcastInstance>(instances);
        List<HazelcastInstance> terminatingInstances = instancesCopy.subList(0, numberOfNodesToStop);
        List<HazelcastInstance> survivingInstances = instancesCopy.subList(numberOfNodesToStop, instances.size());
        populateMaps(survivingInstances.get(0));

        String log = "Surviving: " + survivingInstances + " Terminating: " + terminatingInstances;

        Map<Integer, PartitionReplicaVersionsView> replicaVersionsByPartitionId
                = TestPartitionUtils.getAllReplicaVersions(instances);
        Map<Integer, List<Address>> partitionReplicaAddresses = TestPartitionUtils.getAllReplicaAddresses(instances);
        Map<Integer, Integer> minSurvivingReplicaIndexByPartitionId = getMinReplicaIndicesByPartitionId(survivingInstances);

        stopInstances(terminatingInstances, nodeLeaveType, SAFE_STATE_TIMEOUT_SECONDS);
        waitAllForSafeStateAndDumpPartitionServiceOnFailure(survivingInstances, SAFE_STATE_TIMEOUT_SECONDS);

        validateReplicaVersions(log, numberOfNodesToStop, survivingInstances, replicaVersionsByPartitionId,
                partitionReplicaAddresses, minSurvivingReplicaIndexByPartitionId);
    }

    private void validateReplicaVersions(String log, int numberOfNodesToCrash,
                                         List<HazelcastInstance> survivingInstances,
                                         Map<Integer, PartitionReplicaVersionsView> replicaVersionsByPartitionId,
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
                    PartitionReplicaVersionsView initialReplicaVersions = replicaVersionsByPartitionId.get(partitionId);
                    int minSurvivingReplicaIndex = minSurvivingReplicaIndexByPartitionId.get(partitionId);
                    PartitionReplicaVersionsView
                            replicaVersions = getPartitionReplicaVersionsView(getNode(instance), partitionId);
                    List<Address> addresses = getReplicaAddresses(instance, partitionId);

                    String message = log + " PartitionId: " + partitionId
                            + " InitialReplicaVersions: " + initialReplicaVersions
                            + " ReplicaVersions: " + replicaVersions
                            + " SmallestSurvivingReplicaIndex: " + minSurvivingReplicaIndex
                            + " InitialReplicaAddresses: " + partitionReplicaAddresses.get(partitionId)
                            + " Instance: " + address + " CurrentReplicaAddresses: " + addresses;

                    if (minSurvivingReplicaIndex <= 1) {
                        assertEquals(message, initialReplicaVersions, replicaVersions);
                    } else if (numberOfNodesToCrash > 1) {
                        verifyReplicaVersions(initialReplicaVersions, replicaVersions, minSurvivingReplicaIndex, message);
                    } else {
                        fail(message);
                    }
                }
            }
        }
    }

    private void verifyReplicaVersions(PartitionReplicaVersionsView initialReplicaVersions,
                                       PartitionReplicaVersionsView replicaVersions,
                                       int minSurvivingReplicaIndex, String message) {
        Set<String> lostMapNames = new HashSet<String>();
        for (int i = 0; i < minSurvivingReplicaIndex; i++) {
            lostMapNames.add(getIthMapName(i));
        }

        for (ServiceNamespace namespace : initialReplicaVersions.getNamespaces()) {
            if (replicaVersions.getVersions(namespace) == null) {
                if (namespace instanceof DistributedObjectNamespace) {
                    String objectName = ((DistributedObjectNamespace) namespace).getObjectName();
                    assertThat(objectName, Matchers.isIn(lostMapNames));
                    continue;
                } else {
                    fail("No replica version found for " + namespace);
                }
            }
            verifyReplicaVersions(initialReplicaVersions.getVersions(namespace), replicaVersions.getVersions(namespace),
                    minSurvivingReplicaIndex, message);
        }
    }

    private void verifyReplicaVersions(long[] initialReplicaVersions, long[] replicaVersions,
                                       int minSurvivingReplicaIndex, String message) {
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
    }

    private void shiftLeft(final long[] versions, final int toIndex, final long version) {
        Arrays.fill(versions, 0, toIndex, version);
    }
}
