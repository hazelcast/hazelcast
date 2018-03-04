/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.PartitionLostEvent;
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.util.Preconditions.checkNotNull;

@SuppressFBWarnings("EQ_COMPARETO_USE_OBJECT_EQUALS")
@SuppressWarnings({"checkstyle:methodcount"})
public final class ScheduledFutureProxy<V>
        implements IScheduledFuture<V>, HazelcastInstanceAware {

    private transient HazelcastInstance instance;

    private final transient AtomicBoolean partitionLost = new AtomicBoolean(false);

    private final transient AtomicBoolean memberLost = new AtomicBoolean(false);

    // Single writer, many readers (see partition & member listener)
    private volatile ScheduledTaskHandler handler;

    ScheduledFutureProxy(ScheduledTaskHandler handler, ScheduledExecutorServiceProxy executor) {
        checkNotNull(handler);
        this.handler = handler;
        executor.getService().addLossListener(this);
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.instance = hazelcastInstance;
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

        Operation op = new DisposeTaskOperation(handler);
        InternalCompletableFuture future = invoke(op);
        handler = null;
        future.join();
    }

    void notifyMemberLost(MembershipEvent event) {
        ScheduledTaskHandler handler = this.handler;
        if (handler == null) {
            // Already disposed future
            return;
        }

        if (handler.isAssignedToMember()
                && handler.getAddress().equals(event.getMember().getAddress())) {
            this.memberLost.set(true);
        }
    }

    void notifyPartitionLost(PartitionLostEvent event) {
        ScheduledTaskHandler handler = this.handler;
        if (handler == null) {
            // Already disposed future
            return;
        }

        int durability = instance.getConfig().getScheduledExecutorConfig(handler.getSchedulerName()).getDurability();

        if (handler.isAssignedToPartition()
                && handler.getPartitionId() == event.getPartitionId()
                && event.getLostBackupCount() >= durability) {
            this.partitionLost.set(true);
        }
    }

    private void checkAccessibleOwner() {
        if (handler.isAssignedToPartition()) {
            if (partitionLost.get()) {
                throw new IllegalStateException("Partition " + handler.getPartitionId() + ", holding this scheduled task"
                        + " was lost along with all backups.");
            }
        } else {
            if (memberLost.get()) {
                throw new IllegalStateException("Member with address: " + handler.getAddress() +  ",  holding this scheduled task"
                        + " is not part of this cluster.");
            }
        }
    }

    private void checkAccessibleHandler() {
        if (handler == null) {
            throw new StaleTaskException("Scheduled task was previously disposed.");
        }
    }

    private <T> InternalCompletableFuture<T> invoke(Operation op) {
        if (handler.isAssignedToPartition()) {
            op.setPartitionId(handler.getPartitionId());
            return invokeOnPartition(op);
        } else {
            return invokeOnAddress(op, handler.getAddress());
        }
    }

    private <T> InternalCompletableFuture<T> invokeOnPartition(Operation op) {
        OperationService opService = ((HazelcastInstanceImpl) instance).node.getNodeEngine().getOperationService();

        return opService.invokeOnPartition(op);
    }

    private <T> InternalCompletableFuture<T> invokeOnAddress(Operation op, Address address) {
        OperationService opService = ((HazelcastInstanceImpl) instance).node.getNodeEngine().getOperationService();
        return opService.invokeOnTarget(op.getServiceName(), op, address);
    }

}
