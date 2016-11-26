/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.ScheduledTaskStatistics;
import com.hazelcast.scheduledexecutor.impl.operations.CancelTaskOperation;
import com.hazelcast.scheduledexecutor.impl.operations.CompareToOperation;
import com.hazelcast.scheduledexecutor.impl.operations.DestroyTaskOperation;
import com.hazelcast.scheduledexecutor.impl.operations.GetDelayOperation;
import com.hazelcast.scheduledexecutor.impl.operations.GetResultOperation;
import com.hazelcast.scheduledexecutor.impl.operations.GetStatisticsOperation;
import com.hazelcast.scheduledexecutor.impl.operations.IsCanceledOperation;
import com.hazelcast.scheduledexecutor.impl.operations.IsDoneOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import java.io.IOException;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by Thomas Kountis.
 */
public final class ScheduledFutureProxy<V>
        implements IScheduledFuture<V>,
                   IdentifiedDataSerializable, HazelcastInstanceAware,
                   PartitionLostListener/*, MembershipListener*/ {

    private transient HazelcastInstance instance;

    private transient String partitionLostRegistration;

    private transient boolean partitionLost;

    private ScheduledTaskHandler handler;

    public ScheduledFutureProxy() {
    }

    public ScheduledFutureProxy(ScheduledTaskHandler handler) {
        this.handler = handler;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        unRegisterPartitionListenerIfExists();

        this.instance = hazelcastInstance;
        registerPartitionListener();
    }

    @Override
    public ScheduledTaskHandler getHandler() {
        return handler;
    }

    @Override
    public ScheduledTaskStatistics getStats() {
        checkAccessiblePartition();
        checkAccessibleHandler();

        Operation op = new GetStatisticsOperation(handler);

        return this.<ScheduledTaskStatistics>invoke(op).join();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        checkAccessiblePartition();
        checkAccessibleHandler();

        Operation op = new GetDelayOperation(handler, unit);
        return this.<Long>invoke(op).join();
    }

    @Override
    public int compareTo(Delayed o) {
        checkAccessiblePartition();
        checkAccessibleHandler();

        Operation op = new CompareToOperation(handler, o);
        return this.<Integer>invoke(op).join();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        checkAccessiblePartition();
        checkAccessibleHandler();

        Operation op = new CancelTaskOperation(handler, mayInterruptIfRunning);
        return this.<Boolean>invoke(op).join();
    }

    @Override
    public boolean isCancelled() {
        checkAccessiblePartition();
        checkAccessibleHandler();

        Operation op = new IsCanceledOperation(handler);
        return this.<Boolean>invoke(op).join();
    }

    @Override
    public boolean isDone() {
        checkAccessiblePartition();
        checkAccessibleHandler();

        Operation op = new IsDoneOperation(handler);
        return this.<Boolean>invoke(op).join();
    }

    @Override
    public V get()
            throws InterruptedException, ExecutionException {
        checkAccessiblePartition();
        checkAccessibleHandler();
        Operation op = new GetResultOperation<V>(handler);
        return this.<V>invoke(op).join();
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        checkAccessiblePartition();
        checkAccessibleHandler();
        Operation op = new GetResultOperation<V>(handler);
        return this.<V>invoke(op).get(timeout, unit);
    }

    @Override
    public int getFactoryId() {
        return ScheduledExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ScheduledExecutorDataSerializerHook.SCHEDULED_FUTURE;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeObject(handler);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        handler = in.readObject();
    }

    @Override
    public void destroy() {
        checkAccessiblePartition();
        checkAccessibleHandler();

        unRegisterPartitionListenerIfExists();

        Operation op = new DestroyTaskOperation(handler);
        invoke(op);

        handler = null;
    }

    @Override
    public void partitionLost(PartitionLostEvent event) {
        if (handler.getPartitionId() == event.getPartitionId()) {
            unRegisterPartitionListenerIfExists();
            this.partitionLost = true;
        }
    }

    private void registerPartitionListener() {
        this.partitionLostRegistration = this.instance.getPartitionService().addPartitionLostListener(this);
    }

    private void unRegisterPartitionListenerIfExists() {
        if (partitionLostRegistration != null) {
            this.instance.getPartitionService().removePartitionLostListener(this.partitionLostRegistration);
        }
    }

    private void checkAccessiblePartition() {
        if (partitionLost) {
            throw new IllegalStateException(
                    "Partition holding this Scheduled task was lost along with all backups.");
        }
    }

    private void checkAccessibleHandler() {
        if (handler == null) {
            throw new IllegalStateException(
                    "Scheduled task was previously destroyed.");
        }
    }

    private <V> InternalCompletableFuture<V> invoke(Operation op) {
        if (handler.isAssignedToPartition()) {
            op.setPartitionId(handler.getPartitionId());
            return invokeOnPartition(op);
        } else {
            return invokeOnAddress(op, handler.getAddress());
        }
    }

    private <V> InternalCompletableFuture<V> invokeOnPartition(Operation op) {
        OperationService opService = ((HazelcastInstanceImpl) instance).node
                .getNodeEngine()
                .getOperationService();

        return opService.invokeOnPartition(op);
    }

    private <V> InternalCompletableFuture<V> invokeOnAddress(Operation op, Address address) {
        OperationService opService = ((HazelcastInstanceImpl) instance).node
                .getNodeEngine()
                .getOperationService();

        return opService.invokeOnTarget(op.getServiceName(), op, address);
    }
}
