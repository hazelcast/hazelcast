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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.TargetNotMemberException;

import static com.hazelcast.internal.cluster.Versions.V3_9;

/**
 * Operation sent by the master to the cluster members to fetch their partition state.
 */
public final class FetchPartitionStateOperation extends AbstractPartitionOperation
        implements MigrationCycleOperation {

    private PartitionRuntimeState partitionState;

    public FetchPartitionStateOperation() {
    }

    @Override
    public void run() {
        Address caller = getCallerAddress();
        NodeEngine nodeEngine = getNodeEngine();
        Address master = nodeEngine.getMasterAddress();
        if (!caller.equals(master)) {
            String msg = caller + " requested our partition table but it's not our known master. " + "Master: " + master;
            getLogger().warning(msg);

            ClusterService clusterService = nodeEngine.getClusterService();
            if (clusterService.getClusterVersion().isGreaterOrEqual(V3_9)) {
                throw new IllegalStateException(msg);
            } else {
                throw new RetryableHazelcastException(msg);
            }
        }
        InternalPartitionServiceImpl service = getService();
        partitionState = service.createPartitionStateInternal();
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public Object getResponse() {
        return partitionState;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.FETCH_PARTITION_STATE;
    }
}
