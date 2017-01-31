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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.ScheduledTaskStatistics;
import com.hazelcast.scheduledexecutor.StaleTaskException;
import com.hazelcast.scheduledexecutor.impl.operations.CancelTaskOperation;
import com.hazelcast.scheduledexecutor.impl.operations.DisposeTaskOperation;
import com.hazelcast.scheduledexecutor.impl.operations.GetDelayOperation;
import com.hazelcast.scheduledexecutor.impl.operations.GetResultOperation;
import com.hazelcast.scheduledexecutor.impl.operations.GetStatisticsOperation;
import com.hazelcast.scheduledexecutor.impl.operations.IsCanceledOperation;
import com.hazelcast.scheduledexecutor.impl.operations.IsDoneOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.util.Preconditions.checkNotNull;

@SuppressWarnings({"checkstyle:methodcount"})
public final class ScheduledFutureProxy<V>
        implements IScheduledFuture<V>,
                   HazelcastInstanceAware,
                   PartitionLostListener {

    private transient HazelcastInstance instance;

    private transient String partitionLostRegistration;

    private transient String membershipListenerRegistration;

    private transient boolean partitionLost;

    private transient boolean memberLost;

    private ScheduledTaskHandler handler;

    public ScheduledFutureProxy() {
    }

    public ScheduledFutureProxy(ScheduledTaskHandler handler) {
        this.handler = handler;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        unRegisterPartitionListenerIfExists();
        unRegisterMembershipListenerIfExists();

        this.instance = hazelcastInstance;
        registerPartitionListener();
        registerMembershipListener();
    }

    @Override
    public ScheduledTaskHandler getHandler() {
        return handler;
    }

    @Override
    public ScheduledTaskStatistics getStats() {
        checkAccessibleHandler();
        checkAccessibleOwner();

        Operation op = new GetStatisticsOperation(handler);

        return this.<ScheduledTaskStatistics>invoke(op).join();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        checkNotNull(unit, "Unit is null");
        checkAccessibleHandler();
        checkAccessibleOwner();

        Operation op = new GetDelayOperation(handler, unit);
        return this.<Long>invoke(op).join();
    }

    @Override
    public int compareTo(Delayed o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (mayInterruptIfRunning) {
            // DelegateAndSkipOnConcurrentExecutionDecorator doesn't expose the Executor's future
            // therefore we don't have access to the runner thread to interrupt. We could access through Thread.currentThread()
            // inside the TaskRunner but it adds extra complexity.
            throw new UnsupportedOperationException("mayInterruptIfRunning flag is not supported.");
        }

        checkAccessibleHandler();
        checkAccessibleOwner();

        Operation op = new CancelTaskOperation(handler, mayInterruptIfRunning);
        return this.<Boolean>invoke(op).join();
    }

    @Override
    public boolean isCancelled() {
        checkAccessibleHandler();
        checkAccessibleOwner();

        Operation op = new IsCanceledOperation(handler);
        return this.<Boolean>invoke(op).join();
    }

    @Override
    public boolean isDone() {
        checkAccessibleHandler();
        checkAccessibleOwner();

        Operation op = new IsDoneOperation(handler);
        return this.<Boolean>invoke(op).join();
    }

    private InternalCompletableFuture<V> get0() {
        checkAccessibleHandler();
        checkAccessibleOwner();
        Operation op = new GetResultOperation<V>(handler);
        return this.invoke(op);
    }

    @Override
    public V get()
            throws InterruptedException, ExecutionException {
        try {
            return this.get0().get();
        } catch (ScheduledTaskResult.ExecutionExceptionDecorator ex) {
            return sneakyThrow(ex.getCause());
        }
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        checkNotNull(unit, "Unit is null");
        try {
            return this.get0().get(timeout, unit);
        } catch (ScheduledTaskResult.ExecutionExceptionDecorator ex) {
            return sneakyThrow(ex.getCause());
        }
    }

    @Override
    public void dispose() {
        checkAccessibleHandler();
        checkAccessibleOwner();

        unRegisterPartitionListenerIfExists();
        unRegisterMembershipListenerIfExists();

        Operation op = new DisposeTaskOperation(handler);
        InternalCompletableFuture future = invoke(op);
        handler = null;
        future.join();
    }

    @Override
    public void partitionLost(PartitionLostEvent event) {
        if (handler.getPartitionId() == event.getPartitionId()
                && event.getLostBackupCount() == instance.getConfig().getScheduledExecutorConfig(
                        handler.getSchedulerName()).getDurability()) {
            unRegisterPartitionListenerIfExists();
            this.partitionLost = true;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ScheduledFutureProxy<?> proxy = (ScheduledFutureProxy<?>) o;

        return handler != null ? handler.equals(proxy.handler) : proxy.handler == null;
    }

    @Override
    public int hashCode() {
        return handler != null ? handler.hashCode() : 0;
    }

    private void registerPartitionListener() {
        if (handler.isAssignedToPartition()) {
            this.partitionLostRegistration = this.instance.getPartitionService().addPartitionLostListener(this);
        }
    }

    private void unRegisterPartitionListenerIfExists() {
        if (partitionLostRegistration != null) {
            this.instance.getPartitionService().removePartitionLostListener(this.partitionLostRegistration);
        }
    }

    private void registerMembershipListener() {
        if (handler.isAssignedToMember()) {
            this.membershipListenerRegistration = this.instance.getCluster().addMembershipListener(new MembershipAdapter() {
                @Override
                public void memberRemoved(MembershipEvent membershipEvent) {
                    if (membershipEvent.getMember().getAddress().equals(handler.getAddress())) {
                        ScheduledFutureProxy.this.memberLost = true;
                    }
                }
            });
        }
    }

    private void unRegisterMembershipListenerIfExists() {
        if (membershipListenerRegistration != null) {
            this.instance.getCluster().removeMembershipListener(membershipListenerRegistration);
        }
    }

    private void checkAccessibleOwner() {
        if (handler.isAssignedToPartition()) {
            if (partitionLost) {
                throw new IllegalStateException("Partition holding this Scheduled task was lost along with all backups.");
            }
        } else {
            if (memberLost) {
                throw new IllegalStateException("Member holding this Scheduled task was removed from the cluster.");
            }
        }
    }

    private void checkAccessibleHandler() {
        if (handler == null) {
            throw new StaleTaskException(
                    "Scheduled task was previously disposed.");
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
