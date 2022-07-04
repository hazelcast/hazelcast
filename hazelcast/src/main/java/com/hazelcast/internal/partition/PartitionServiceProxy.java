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

import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.operation.SafeStateCheckOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.internal.util.FutureUtil;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class PartitionServiceProxy implements PartitionService {

    private final NodeEngineImpl nodeEngine;
    private final InternalPartitionServiceImpl partitionService;
    private final Map<Integer, Partition> partitionMap;
    private final Set<Partition> partitionSet;
    private final ILogger logger;

    private final FutureUtil.ExceptionHandler exceptionHandler = new FutureUtil.ExceptionHandler() {
        @Override
        public void handleException(Throwable e) {
            logger.warning("Error while querying cluster's safe state", e);
        }
    };

    public PartitionServiceProxy(NodeEngineImpl nodeEngine, InternalPartitionServiceImpl partitionService) {
        this.nodeEngine = nodeEngine;
        this.partitionService = partitionService;

        int partitionCount = partitionService.getPartitionCount();
        Map<Integer, Partition> map = createHashMap(partitionCount);
        Set<Partition> set = new TreeSet<>();
        for (int i = 0; i < partitionCount; i++) {
            Partition partition = new PartitionProxy(i);
            set.add(partition);
            map.put(i, partition);
        }

        partitionMap = Collections.unmodifiableMap(map);
        partitionSet = Collections.unmodifiableSet(set);
        logger = nodeEngine.getLogger(PartitionServiceProxy.class);
    }

    @Override
    public Set<Partition> getPartitions() {
        return partitionSet;
    }

    @Override
    public Partition getPartition(@Nonnull Object key) {
        checkNotNull(key, "key cannot be null");
        int partitionId = partitionService.getPartitionId(key);
        return partitionMap.get(partitionId);
    }

    @Override
    public UUID addMigrationListener(final MigrationListener migrationListener) {
        return partitionService.addMigrationListener(migrationListener);
    }

    @Override
    public boolean removeMigrationListener(final UUID registrationId) {
        return partitionService.removeMigrationListener(registrationId);
    }

    @Override
    public UUID addPartitionLostListener(PartitionLostListener partitionLostListener) {
        return partitionService.addPartitionLostListener(partitionLostListener);
    }

    @Override
    public boolean removePartitionLostListener(UUID registrationId) {
        return partitionService.removePartitionLostListener(registrationId);
    }

    @Override
    public boolean isClusterSafe() {
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        if (members == null || members.isEmpty()) {
            return true;
        }

        final Collection<Future<Boolean>> futures = new ArrayList<>(members.size());
        for (Member member : members) {
            final Address target = member.getAddress();
            final Operation operation = new SafeStateCheckOperation();
            final Future<Boolean> future = nodeEngine.getOperationService()
                    .invokeOnTarget(InternalPartitionService.SERVICE_NAME, operation, target);
            futures.add(future);
        }

        // todo this max wait is appropriate?
        final int maxWaitTime = getMaxWaitTime();
        Collection<Boolean> results =  FutureUtil.returnWithDeadline(futures, maxWaitTime, TimeUnit.SECONDS,
                exceptionHandler);

        if (results.size() != futures.size()) {
            return false;
        }
        for (Boolean result : results) {
            if (!result) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isMemberSafe(Member member) {
        if (member == null) {
            throw new NullPointerException("Parameter member must not be null");
        }
        final Member localMember = nodeEngine.getLocalMember();
        if (localMember.equals(member)) {
            return isLocalMemberSafe();
        }
        final Address target = member.getAddress();
        final Operation operation = new SafeStateCheckOperation();
        final InvocationFuture future = nodeEngine.getOperationService()
                                                  .invokeOnTarget(InternalPartitionService.SERVICE_NAME, operation, target);
        boolean safe;
        try {
            final Object result = future.get(10, TimeUnit.SECONDS);
            safe = (Boolean) result;
        } catch (Throwable t) {
            safe = false;
            logger.warning("Error while querying member's safe state [" + member + "]", t);
        }
        return safe;
    }

    @Override
    public boolean isLocalMemberSafe() {
        if (!nodeActive()) {
            return true;
        }
        return partitionService.isMemberStateSafe();
    }

    @Override
    public boolean forceLocalMemberToBeSafe(long timeout, TimeUnit unit) {
        if (unit == null) {
            throw new NullPointerException();
        }

        if (timeout < 1L) {
            throw new IllegalArgumentException();
        }

        if (!nodeActive()) {
            return true;
        }
        return partitionService.getPartitionReplicaStateChecker().triggerAndWaitForReplicaSync(timeout, unit);
    }

    private boolean nodeActive() {
        return nodeEngine.getNode().getState() != NodeState.SHUT_DOWN;
    }

    private int getMaxWaitTime() {
        return nodeEngine.getProperties().getSeconds(ClusterProperty.GRACEFUL_SHUTDOWN_MAX_WAIT);
    }

    private class PartitionProxy implements Partition, Comparable {

        final int partitionId;

        PartitionProxy(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public Member getOwner() {
            // triggers initial partition assignment
            InternalPartition partition = partitionService.getPartition(partitionId);
            PartitionReplica owner = partition.getOwnerReplicaOrNull();
            if (owner == null) {
                return null;
            }

            return nodeEngine.getClusterService().getMember(owner.address(), owner.uuid());
        }

        @Override
        public int compareTo(Object o) {
            PartitionProxy partition = (PartitionProxy) o;
            return Integer.compare(partitionId, partition.partitionId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartitionProxy partition = (PartitionProxy) o;
            return partitionId == partition.partitionId;
        }

        @Override
        public int hashCode() {
            return partitionId;
        }

        @Override
        public String toString() {
            return "Partition [" + partitionId + "], owner=" + getOwner();
        }
    }
}
