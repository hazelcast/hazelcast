/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Member;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.operation.SafeStateCheckOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.util.FutureUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.MapUtil.createHashMap;

public class PartitionServiceProxy implements PartitionService {

    private final NodeEngineImpl nodeEngine;
    private final InternalPartitionServiceImpl partitionService;
    private final Map<Integer, Partition> partitionMap;
    private final Set<Partition> partitionSet;
    private final Random random = new Random();
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
        Set<Partition> set = new TreeSet<Partition>();
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
    public String randomPartitionKey() {
        return Integer.toString(random.nextInt(partitionService.getPartitionCount()));
    }

    @Override
    public Set<Partition> getPartitions() {
        return partitionSet;
    }

    @Override
    public Partition getPartition(Object key) {
        int partitionId = partitionService.getPartitionId(key);
        return partitionMap.get(partitionId);
    }

    @Override
    public String addMigrationListener(final MigrationListener migrationListener) {
        return partitionService.addMigrationListener(migrationListener);
    }

    @Override
    public boolean removeMigrationListener(final String registrationId) {
        return partitionService.removeMigrationListener(registrationId);
    }

    @Override
    public String addPartitionLostListener(PartitionLostListener partitionLostListener) {
        return partitionService.addPartitionLostListener(partitionLostListener);
    }

    @Override
    public boolean removePartitionLostListener(String registrationId) {
        return partitionService.removePartitionLostListener(registrationId);
    }

    @Override
    public boolean isClusterSafe() {
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        if (members == null || members.isEmpty()) {
            return true;
        }

        final Collection<Future<Boolean>> futures = new ArrayList<Future<Boolean>>(members.size());
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
            throw new NullPointerException("Parameter member should not be null");
        }
        final Member localMember = nodeEngine.getLocalMember();
        if (localMember.equals(member)) {
            return isLocalMemberSafe();
        }
        final Address target = member.getAddress();
        final Operation operation = new SafeStateCheckOperation();
        final InternalCompletableFuture future = nodeEngine.getOperationService()
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
        return nodeEngine.getProperties().getSeconds(GroupProperty.GRACEFUL_SHUTDOWN_MAX_WAIT);
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
            final Address address = partitionService.getPartitionOwner(partitionId);
            if (address == null) {
                return null;
            }

            return nodeEngine.getClusterService().getMember(address);
        }

        @Override
        public int compareTo(Object o) {
            PartitionProxy partition = (PartitionProxy) o;
            Integer id = partitionId;
            return (id.compareTo(partition.getPartitionId()));
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
