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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService.SERVICE_NAME;

/**
 * Container for member owned Schedulers. Member operations don't adhere to the same threading model as partition ones, and they
 * require some guarding around them.
 */
public class ScheduledExecutorMemberOwnedContainer
        extends ScheduledExecutorContainer {

    private static final int MEMBER_DURABILITY = 0;

    private final AtomicBoolean memberPartitionLock = new AtomicBoolean();

    ScheduledExecutorMemberOwnedContainer(String name, CapacityPermit permit,
                                          NodeEngine nodeEngine, boolean statisticsEnabled) {
        super(name, -1, nodeEngine, permit, MEMBER_DURABILITY,
                new ConcurrentHashMap<>(), statisticsEnabled);
    }

    @Override
    public ScheduledFuture schedule(TaskDefinition definition) {
        try {
            acquireMemberPartitionLockIfNeeded();
            return super.schedule(definition);
        } finally {
            releaseMemberPartitionLockIfNeeded();
        }

    }

    @Override
    public boolean shouldParkGetResult(String taskName) {
        // For member owned tasks there is a race condition, so we avoid purposefully parking.
        // TODO tkountis - Look into the invocation subsystem, to identify root cause.
        return false;
    }

    @Override
    public ScheduledTaskHandler offprintHandler(String taskName) {
        return ScheduledTaskHandlerImpl.of(getNodeEngine().getClusterService().getLocalMember().getUuid(), getName(), taskName);
    }

    @Override
    protected InvocationBuilder createInvocationBuilder(Operation op) {
        OperationService operationService = getNodeEngine().getOperationService();
        return operationService.createInvocationBuilder(SERVICE_NAME, op, getNodeEngine().getThisAddress());
    }

    private void acquireMemberPartitionLockIfNeeded() {
        while (!memberPartitionLock.compareAndSet(false, true)) {
            Thread.yield();
        }
    }

    private void releaseMemberPartitionLockIfNeeded() {
        memberPartitionLock.set(false);
    }

}
