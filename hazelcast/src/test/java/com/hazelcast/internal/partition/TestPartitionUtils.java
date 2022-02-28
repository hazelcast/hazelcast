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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionReplicaManager;
import com.hazelcast.internal.partition.impl.PartitionServiceState;
import com.hazelcast.internal.partition.impl.ReplicaFragmentSyncInfo;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.internal.util.scheduler.ScheduledEntry;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.impl.TestUtil.getNode;
import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getPartitionService;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyObjectForStarter;
import static com.hazelcast.test.starter.HazelcastStarterUtils.rethrowGuardianException;
import static com.hazelcast.test.starter.ReflectionUtils.getDelegateFromMock;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;
import static java.lang.reflect.Proxy.isProxyClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("WeakerAccess")
public final class TestPartitionUtils {

    private TestPartitionUtils() {
    }

    public static PartitionServiceState getPartitionServiceState(HazelcastInstance instance) {
        try {
            Node node = getNode(instance);
            InternalPartitionService partitionService = node.getPartitionService();
            if (isProxyClass(instance.getClass())) {
                try {
                    // this is very ugly, but because of a direct field access to Node.nodeEngine in the MigrationManager
                    // constructor, we cannot properly mock or proxy the PartitionReplicaStateChecker class
                    Object delegate = getDelegateFromMock(partitionService);
                    Object partitionReplicaStateChecker = getFieldValueReflectively(delegate, "partitionReplicaStateChecker");
                    Method method = partitionReplicaStateChecker.getClass().getMethod("getPartitionServiceState");
                    Object result = method.invoke(partitionReplicaStateChecker);
                    return (PartitionServiceState) proxyObjectForStarter(TestPartitionUtils.class.getClassLoader(), result);
                } catch (Exception e) {
                    throw rethrowGuardianException(e);
                }
            } else {
                return partitionService.getPartitionReplicaStateChecker().getPartitionServiceState();
            }
        } catch (IllegalArgumentException e) {
            return PartitionServiceState.SAFE;
        }
    }

    public static Map<Integer, PartitionReplicaVersionsView> getAllReplicaVersions(List<HazelcastInstance> instances) {
        Map<Integer, PartitionReplicaVersionsView> replicaVersions = new HashMap<Integer, PartitionReplicaVersionsView>();
        for (HazelcastInstance instance : instances) {
            collectOwnedReplicaVersions(getNode(instance), replicaVersions);
        }
        return replicaVersions;
    }

    public static Map<Integer, PartitionReplicaVersionsView> getOwnedReplicaVersions(Node node) {
        Map<Integer, PartitionReplicaVersionsView> ownedReplicaVersions = new HashMap<Integer, PartitionReplicaVersionsView>();
        collectOwnedReplicaVersions(node, ownedReplicaVersions);
        return ownedReplicaVersions;
    }

    private static void collectOwnedReplicaVersions(Node node, Map<Integer, PartitionReplicaVersionsView> replicaVersions) {
        InternalPartitionService partitionService = node.getPartitionService();
        Address nodeAddress = node.getThisAddress();
        for (IPartition partition : partitionService.getPartitions()) {
            if (nodeAddress.equals(partition.getOwnerOrNull())) {
                int partitionId = partition.getPartitionId();
                replicaVersions.put(partitionId, getPartitionReplicaVersionsView(node, partitionId));
            }
        }
    }

    public static long[] getDefaultReplicaVersions(Node node, int partitionId) {
        return getPartitionReplicaVersionsView(node, partitionId).getVersions(NonFragmentedServiceNamespace.INSTANCE);
    }

    public static PartitionReplicaVersionsView getPartitionReplicaVersionsView(Node node, int partitionId) {
        GetReplicaVersionsRunnable runnable = new GetReplicaVersionsRunnable(node, partitionId);
        node.getNodeEngine().getOperationService().execute(runnable);
        return runnable.getReplicaVersions();
    }

    public static List<ReplicaFragmentSyncInfo> getOngoingReplicaSyncRequests(HazelcastInstance instance) {
        return getOngoingReplicaSyncRequests(getNode(instance));
    }

    public static List<ReplicaFragmentSyncInfo> getOngoingReplicaSyncRequests(Node node) {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) node.getPartitionService();
        return partitionService.getOngoingReplicaSyncRequests();
    }

    public static List<ScheduledEntry<ReplicaFragmentSyncInfo, Void>> getScheduledReplicaSyncRequests(HazelcastInstance instance) {
        return getScheduledReplicaSyncRequests(getNode(instance));
    }

    public static List<ScheduledEntry<ReplicaFragmentSyncInfo, Void>> getScheduledReplicaSyncRequests(Node node) {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) node.getPartitionService();
        return partitionService.getScheduledReplicaSyncRequests();
    }

    public static Map<Integer, List<Address>> getAllReplicaAddresses(List<HazelcastInstance> instances) {
        if (instances.isEmpty()) {
            return Collections.emptyMap();
        }

        for (HazelcastInstance instance : instances) {
            Node node = getNode(instance);
            if (node.isMaster()) {
                return getAllReplicaAddresses(node);
            }
        }

        return Collections.emptyMap();
    }

    public static Map<Integer, List<Address>> getAllReplicaAddresses(HazelcastInstance instance) {
        return getAllReplicaAddresses(getNode(instance));
    }

    public static Map<Integer, List<Address>> getAllReplicaAddresses(Node node) {
        Map<Integer, List<Address>> allReplicaAddresses = new HashMap<Integer, List<Address>>();
        InternalPartitionService partitionService = node.getPartitionService();
        for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
            allReplicaAddresses.put(partitionId, getReplicaAddresses(node, partitionId));
        }

        return allReplicaAddresses;
    }

    public static List<Address> getReplicaAddresses(HazelcastInstance instance, int partitionId) {
        return getReplicaAddresses(getNode(instance), partitionId);
    }

    public static List<Address> getReplicaAddresses(Node node, int partitionId) {
        List<Address> replicaAddresses = new ArrayList<Address>();
        InternalPartitionService partitionService = node.getPartitionService();
        InternalPartition partition = partitionService.getPartition(partitionId);
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            replicaAddresses.add(partition.getReplicaAddress(i));
        }
        return replicaAddresses;
    }

    private static class GetReplicaVersionsRunnable implements PartitionSpecificRunnable {

        private final CountDownLatch latch = new CountDownLatch(1);

        private final Node node;
        private final int partitionId;

        private PartitionReplicaVersionsView replicaVersions;

        GetReplicaVersionsRunnable(Node node, int partitionId) {
            this.node = node;
            this.partitionId = partitionId;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            InternalPartitionService partitionService = node.nodeEngine.getPartitionService();
            PartitionReplicaManager replicaManager =
                    (PartitionReplicaManager) partitionService.getPartitionReplicaVersionManager();

            Collection<ServiceNamespace> namespaces = replicaManager.getNamespaces(partitionId);
            Map<ServiceNamespace, long[]> versionMap = new HashMap<>(namespaces.size());
            Set<ServiceNamespace> dirty = new HashSet<ServiceNamespace>();
            for (ServiceNamespace ns : namespaces) {
                long[] originalVersions = replicaManager.getPartitionReplicaVersions(partitionId, ns);
                long[] versions = Arrays.copyOf(originalVersions, originalVersions.length);
                versionMap.put(ns, versions);

                if (replicaManager.isPartitionReplicaVersionDirty(partitionId, ns)) {
                    dirty.add(ns);
                }
            }

            if (!versionMap.containsKey(NonFragmentedServiceNamespace.INSTANCE)) {
                versionMap.put(NonFragmentedServiceNamespace.INSTANCE, new long[InternalPartition.MAX_BACKUP_COUNT]);
            }

            replicaVersions = new PartitionReplicaVersionsView(versionMap, dirty);
            latch.countDown();
        }

        PartitionReplicaVersionsView getReplicaVersions() {
            assertOpenEventually(latch, TimeUnit.MINUTES.toSeconds(1));
            return replicaVersions;
        }
    }

    public static void assertAllPartitionsBelongTo(HazelcastInstance instance) {
        InternalPartitionService internalPartitionService = getPartitionService(instance);
        int partitionCount = internalPartitionService.getPartitionCount();
        assertTrue(internalPartitionService.getMemberPartitionsIfAssigned(getAddress(instance)).size() == partitionCount);
    }

    public static void assertSomePartitionsBelongTo(HazelcastInstance instance, Address address) {
        InternalPartitionService internalPartitionService = getPartitionService(instance);
        assertTrue(internalPartitionService.getMemberPartitionsIfAssigned(address).size() > 0);
    }

    public static void assertSomePartitionsBelongTo(HazelcastInstance instance) {
        InternalPartitionService internalPartitionService = getPartitionService(instance);
        assertTrue(internalPartitionService.getMemberPartitionsIfAssigned(getAddress(instance)).size() > 0);
    }

    public static void assertNoPartitionsBelongTo(HazelcastInstance instance) {
        InternalPartitionService internalPartitionService = getPartitionService(instance);
        assertEquals(0, internalPartitionService.getMemberPartitionsIfAssigned(getAddress(instance)).size());
    }

    public static String dumpPartitionTable(PartitionTableView partitionTableView) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partitionTableView.length(); i++) {
            sb.append(i).append(" -> [")
              .append(Arrays.toString(partitionTableView.getReplicas(i))).append("]\n");
        }
        return sb.toString();
    }
}
