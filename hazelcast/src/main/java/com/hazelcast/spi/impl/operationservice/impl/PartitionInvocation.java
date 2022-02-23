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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.spi.impl.operationservice.ExceptionAction.THROW_EXCEPTION;

/**
 * A {@link Invocation} evaluates a Operation Invocation for a particular partition running on top of the
 * {@link OperationServiceImpl}.
 */
final class PartitionInvocation extends Invocation<PartitionReplica> {

    private final boolean failOnIndeterminateOperationState;

    PartitionInvocation(Context context,
                        Operation op,
                        Runnable doneCallback,
                        int tryCount,
                        long tryPauseMillis,
                        long callTimeoutMillis,
                        boolean deserialize,
                        boolean failOnIndeterminateOperationState,
                        ServerConnectionManager connectionManager) {
        super(context, op, doneCallback, tryCount, tryPauseMillis, callTimeoutMillis, deserialize, connectionManager);
        this.failOnIndeterminateOperationState = failOnIndeterminateOperationState && !(op instanceof ReadonlyOperation);
    }

    PartitionInvocation(Context context,
                        Operation op,
                        int tryCount,
                        long tryPauseMillis,
                        long callTimeoutMillis,
                        boolean deserialize,
                        boolean failOnIndeterminateOperationState) {
        this(context, op, null, tryCount, tryPauseMillis, callTimeoutMillis, deserialize,
                failOnIndeterminateOperationState, null);
    }

    @Override
    PartitionReplica getInvocationTarget() {
        InternalPartition partition = context.partitionService.getPartition(op.getPartitionId());
        return partition.getReplica(op.getReplicaIndex());
    }

    @Override
    Address toTargetAddress(PartitionReplica replica) {
        return replica.address();
    }

    @Override
    Member toTargetMember(PartitionReplica replica) {
        return context.clusterService.getMember(replica.address(), replica.uuid());
    }

    @Override
    Exception newTargetNullException() {
        ClusterState clusterState = context.clusterService.getClusterState();
        if (!clusterState.isMigrationAllowed()) {
            return new IllegalStateException("Target of invocation cannot be found! Partition owner is null "
                    + "but partitions can't be assigned in cluster-state: " + clusterState);
        }
        if (context.clusterService.getSize(DATA_MEMBER_SELECTOR) == 0) {
            return new NoDataMemberInClusterException(
                    "Target of invocation cannot be found! Partition owner is null "
                            + "but partitions can't be assigned since all nodes in the cluster are lite members.");
        }
        return super.newTargetNullException();
    }

    @Override
    protected boolean shouldFailOnIndeterminateOperationState() {
        return failOnIndeterminateOperationState;
    }

    @Override
    ExceptionAction onException(Throwable t) {
        if (shouldFailOnIndeterminateOperationState() && (t instanceof MemberLeftException)) {
            return THROW_EXCEPTION;
        }

        ExceptionAction action = op.onInvocationException(t);
        return action != null ? action : THROW_EXCEPTION;
    }
}
