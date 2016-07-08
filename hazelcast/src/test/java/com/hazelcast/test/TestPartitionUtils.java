package com.hazelcast.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionServiceState;
import com.hazelcast.internal.partition.impl.ReplicaSyncInfo;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.util.scheduler.ScheduledEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.TestUtil.getNode;
import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;

public class TestPartitionUtils {

    private TestPartitionUtils() {
    }

    public static PartitionServiceState getPartitionServiceState(HazelcastInstance instance) {
        return getPartitionServiceState(getNode(instance));
    }

    public static PartitionServiceState getPartitionServiceState(Node node) {
        if (node == null) {
            return PartitionServiceState.SAFE;
        }
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) node.getPartitionService();
        return partitionService.getPartitionReplicaStateChecker().getPartitionServiceState();
    }

    public static Map<Integer, long[]> getAllReplicaVersions(List<HazelcastInstance> instances) throws InterruptedException {
        Map<Integer, long[]> replicaVersions = new HashMap<Integer, long[]>();
        for (HazelcastInstance instance : instances) {
            collectOwnedReplicaVersions(getNode(instance), replicaVersions);
        }
        return replicaVersions;
    }

    public static Map<Integer, long[]> getOwnedReplicaVersions(HazelcastInstance instance) throws InterruptedException {
        return getOwnedReplicaVersions(getNode(instance));
    }

    public static Map<Integer, long[]> getOwnedReplicaVersions(Node node) throws InterruptedException {
        Map<Integer, long[]> ownedReplicaVersions = new HashMap<Integer, long[]>();
        collectOwnedReplicaVersions(node, ownedReplicaVersions);
        return ownedReplicaVersions;
    }

    private static void collectOwnedReplicaVersions(Node node, Map<Integer, long[]> replicaVersions) throws InterruptedException {
        InternalPartitionService partitionService = node.getPartitionService();
        Address nodeAddress = node.getThisAddress();
        for (IPartition partition : partitionService.getPartitions()) {
            if (nodeAddress.equals(partition.getOwnerOrNull())) {
                int partitionId = partition.getPartitionId();
                replicaVersions.put(partitionId, getReplicaVersions(node, partitionId));
            }
        }
    }

    public static long[] getReplicaVersions(HazelcastInstance instance, int partitionId) throws InterruptedException {
        return getReplicaVersions(getNode(instance), partitionId);
    }

    public static long[] getReplicaVersions(Node node, int partitionId) throws InterruptedException {
        GetReplicaVersionsRunnable runnable = new GetReplicaVersionsRunnable(node, partitionId);
        node.getNodeEngine().getOperationService().execute(runnable);
        return runnable.getReplicaVersions();
    }

    public static List<ReplicaSyncInfo> getOngoingReplicaSyncRequests(HazelcastInstance instance) {
        return getOngoingReplicaSyncRequests(getNode(instance));
    }

    public static List<ReplicaSyncInfo> getOngoingReplicaSyncRequests(Node node) {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) node.getPartitionService();
        return partitionService.getOngoingReplicaSyncRequests();
    }

    public static List<ScheduledEntry<Integer, ReplicaSyncInfo>> getScheduledReplicaSyncRequests(HazelcastInstance instance) {
        return getScheduledReplicaSyncRequests(getNode(instance));
    }

    public static List<ScheduledEntry<Integer, ReplicaSyncInfo>> getScheduledReplicaSyncRequests(Node node) {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) node.getPartitionService();
        return partitionService.getScheduledReplicaSyncRequests();
    }

    public static Map<Integer, List<Address>> getAllReplicaAddresses(List<HazelcastInstance> instances) {
        if (instances.isEmpty()) {
            return Collections.emptyMap();
        }

        for (HazelcastInstance instance : instances) {
            Node node = getNode(instance);
            if (node != null && node.isMaster()) {
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

        private long[] replicaVersions;

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
            long[] replicaVersionsArray = partitionService.getPartitionReplicaVersions(partitionId);
            replicaVersions = Arrays.copyOf(replicaVersionsArray, replicaVersionsArray.length);
            latch.countDown();
        }

        long[] getReplicaVersions() throws InterruptedException {
            latch.await(30, TimeUnit.SECONDS);
            return replicaVersions;
        }
    }
}
