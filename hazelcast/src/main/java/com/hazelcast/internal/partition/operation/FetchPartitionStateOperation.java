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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Operation sent by the master to the cluster members to fetch their partition state.
 */
public final class FetchPartitionStateOperation extends AbstractPartitionOperation
        implements MigrationCycleOperation {

    public FetchPartitionStateOperation() {
    }

    @Override
    public void beforeRun() {
        Address caller = getCallerAddress();
        Address masterAddress = getNodeEngine().getMasterAddress();
        ILogger logger = getLogger();
        if (!caller.equals(masterAddress)) {
            String msg = caller + " requested our partition table but it's not our known master. " + "Master: " + masterAddress;
            logger.warning(msg);
            // Master address should be already updated after mastership claim.
            throw new IllegalStateException(msg);
        }

        InternalPartitionServiceImpl service = getService();
        if (!service.isMemberMaster(caller)) {
            String msg = caller + " requested our partition table but it's not the master known by migration system.";
            logger.warning(msg);
            // PartitionService has not received result of mastership claim process yet.
            // It will learn eventually.
            throw new RetryableHazelcastException(msg);
        }
    }

    @Override
    public CallStatus call() {
        return new OffloadImpl();
    }

    private final class OffloadImpl extends Offload {
        private OffloadImpl() {
            super(FetchPartitionStateOperation.this);
        }

        @Override
        public void start() {
            NodeEngine nodeEngine = getNodeEngine();
            OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
            OperationExecutor executor = operationService.getOperationExecutor();

            int partitionThreadCount = executor.getPartitionThreadCount();
            SendPartitionStateTask barrierTask = new SendPartitionStateTask(partitionThreadCount);
            executor.executeOnPartitionThreads(barrierTask);
        }
    }

    /**
     * SendPartitionStateTask is executed on all partition operation threads
     * and sends local partition state to the caller (the master) after
     * ensuring all pending/running migration operations are completed.
     */
    private final class SendPartitionStateTask implements Runnable, UrgentSystemOperation {
        private final AtomicInteger remaining = new AtomicInteger();

        private SendPartitionStateTask(int partitionThreadCount) {
            remaining.set(partitionThreadCount);
        }

        @Override
        public void run() {
            if (remaining.decrementAndGet() == 0) {
                InternalPartitionServiceImpl service = getService();
                PartitionRuntimeState partitionState = service.createPartitionStateInternal();
                sendResponse(partitionState);
            }
        }
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException
                || throwable instanceof TargetNotMemberException
                || throwable instanceof CallerNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.FETCH_PARTITION_STATE;
    }
}
