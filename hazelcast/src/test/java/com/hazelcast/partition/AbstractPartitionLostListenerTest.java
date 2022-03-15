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

package com.hazelcast.partition;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplicaVersionsView;
import com.hazelcast.internal.partition.impl.ReplicaFragmentSyncInfo;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.scheduler.ScheduledEntry;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.partition.TestPartitionUtils.getAllReplicaAddresses;
import static com.hazelcast.internal.partition.TestPartitionUtils.getOngoingReplicaSyncRequests;
import static com.hazelcast.internal.partition.TestPartitionUtils.getOwnedReplicaVersions;
import static com.hazelcast.internal.partition.TestPartitionUtils.getScheduledReplicaSyncRequests;
import static com.hazelcast.test.Accessors.getNode;
import static junit.framework.TestCase.assertNotNull;

@SuppressWarnings("WeakerAccess")
public abstract class AbstractPartitionLostListenerTest extends HazelcastTestSupport {

    public enum NodeLeaveType {
        SHUTDOWN,
        TERMINATE
    }

    private TestHazelcastInstanceFactory hazelcastInstanceFactory;

    @Before
    public final void setup() {
        hazelcastInstanceFactory = createHazelcastInstanceFactory(getNodeCount());
    }

    @After
    public final void tearDown() {
        hazelcastInstanceFactory.terminateAll();
    }

    protected abstract int getNodeCount();

    protected int getMapEntryCount() {
        return 0;
    }

    protected int getMaxParallelReplicaSyncCount() {
        return 20;
    }

    protected final void stopInstances(List<HazelcastInstance> instances, NodeLeaveType nodeLeaveType) {
        stopInstances(instances, nodeLeaveType, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    protected final void stopInstances(List<HazelcastInstance> instances, final NodeLeaveType nodeLeaveType, int timeoutSeconds) {
        assertNotNull(nodeLeaveType);

        final List<Thread> threads = new ArrayList<Thread>();
        final CountDownLatch latch = new CountDownLatch(instances.size());
        for (final HazelcastInstance instance : instances) {
            threads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    if (nodeLeaveType == NodeLeaveType.SHUTDOWN) {
                        instance.getLifecycleService().shutdown();
                        latch.countDown();
                    } else if (nodeLeaveType == NodeLeaveType.TERMINATE) {
                        instance.getLifecycleService().terminate();
                        latch.countDown();
                    } else {
                        System.err.println("Invalid node leave type: " + nodeLeaveType);
                    }
                }
            }));
        }

        for (Thread t : threads) {
            t.start();
        }

        assertOpenEventually(latch, timeoutSeconds);
    }

    protected final List<HazelcastInstance> getCreatedInstancesShuffledAfterWarmedUp() {
        return getCreatedInstancesShuffledAfterWarmedUp(getNodeCount());
    }

    protected final List<HazelcastInstance> getCreatedInstancesShuffledAfterWarmedUp(int nodeCount) {
        List<HazelcastInstance> instances = createInstances(nodeCount);
        warmUpPartitions(instances);
        Collections.shuffle(instances);
        return instances;
    }

    protected final List<HazelcastInstance> createInstances(int nodeCount) {
        List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>();
        Config config = createConfig(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            instances.add(hazelcastInstanceFactory.newHazelcastInstance(config));
        }
        return instances;
    }

    private Config createConfig(int nodeCount) {
        Config config = getConfig();
        config.setProperty("hazelcast.partition.max.parallel.replications", Integer.toString(getMaxParallelReplicaSyncCount()));
        for (int i = 0; i < nodeCount; i++) {
            config.getMapConfig(getIthMapName(i)).setBackupCount(Math.min(i, InternalPartition.MAX_BACKUP_COUNT));
        }
        return config;
    }

    protected final void populateMaps(HazelcastInstance instance) {
        for (int i = 0; i < getNodeCount(); i++) {
            Map<Integer, Integer> map = instance.getMap(getIthMapName(i));
            for (int j = 0; j < getMapEntryCount(); j++) {
                map.put(j, j);
            }
        }
    }

    protected final String getIthMapName(int i) {
        return "map-" + i;
    }

    protected final String getIthCacheName(int i) {
        return "cache-" + i;
    }

    protected final Map<Integer, Integer> getMinReplicaIndicesByPartitionId(List<HazelcastInstance> instances) {
        Map<Integer, Integer> survivingPartitions = new HashMap<Integer, Integer>();
        for (HazelcastInstance instance : instances) {
            Node survivingNode = getNode(instance);
            Address survivingNodeAddress = survivingNode.getThisAddress();

            for (IPartition partition : survivingNode.getPartitionService().getPartitions()) {
                if (partition.isOwnerOrBackup(survivingNodeAddress)) {
                    for (int replicaIndex = 0; replicaIndex < getNodeCount(); replicaIndex++) {
                        if (survivingNodeAddress.equals(partition.getReplicaAddress(replicaIndex))) {
                            Integer replicaIndexOfOtherInstance = survivingPartitions.get(partition.getPartitionId());
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

    @SuppressWarnings("SameParameterValue")
    protected final void waitAllForSafeStateAndDumpPartitionServiceOnFailure(List<HazelcastInstance> instances,
                                                                             int timeoutInSeconds) {
        try {
            waitAllForSafeState(instances, timeoutInSeconds);
        } catch (AssertionError e) {
            logPartitionState(instances);
            throw e;
        }
    }

    private void logPartitionState(List<HazelcastInstance> instances) {
        for (Entry<Integer, List<Address>> entry : getAllReplicaAddresses(instances).entrySet()) {
            System.out.println("PartitionTable >> partitionId=" + entry.getKey() + " table=" + entry.getValue());
        }

        for (HazelcastInstance instance : instances) {
            Address address = getNode(instance).getThisAddress();
            for (Entry<Integer, PartitionReplicaVersionsView> entry : getOwnedReplicaVersions(getNode(instance)).entrySet()) {
                PartitionReplicaVersionsView replicaVersionsView = entry.getValue();
                for (ServiceNamespace namespace : replicaVersionsView.getNamespaces()) {
                    System.out.println(namespace + " ReplicaVersions >> " + address + " - partitionId=" + entry.getKey()
                            + " replicaVersions=" + Arrays.toString(replicaVersionsView.getVersions(namespace)));
                }
            }

            for (ReplicaFragmentSyncInfo replicaSyncInfo : getOngoingReplicaSyncRequests(instance)) {
                System.out.println("OngoingReplicaSync >> " + address + " - " + replicaSyncInfo);
            }

            for (ScheduledEntry<ReplicaFragmentSyncInfo, Void> entry : getScheduledReplicaSyncRequests(instance)) {
                System.out.println("ScheduledReplicaSync >> " + address + " - " + entry.getKey());
            }
        }
    }
}
