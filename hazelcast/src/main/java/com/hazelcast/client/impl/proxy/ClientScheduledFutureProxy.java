/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorCancelFromAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorCancelFromPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorDisposeFromAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorDisposeFromPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetDelayFromAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetDelayFromPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetResultFromAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetResultFromPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetStatsFromAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetStatsFromPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsCancelledFromAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsCancelledFromPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsDoneFromAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsDoneFromPartitionCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.ScheduledTaskStatistics;
import com.hazelcast.scheduledexecutor.StaleTaskException;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskStatisticsImpl;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Proxy implementation of {@link IScheduledFuture}.
 *
 * @param <V> the return type of the {@link Callable#call()}
 */
public class ClientScheduledFutureProxy<V>
        extends ClientProxy
        implements IScheduledFuture<V> {

    private ScheduledTaskHandler handler;

    ClientScheduledFutureProxy(ScheduledTaskHandler handler, ClientContext context) {
        super(DistributedScheduledExecutorService.SERVICE_NAME, handler.getSchedulerName(), context);
        this.handler = handler;
    }

    @Override
    public ScheduledTaskHandler getHandler() {
        return handler;
    }

    @Override
    public ScheduledTaskStatistics getStats() {
        checkAccessibleHandler();
        Address address = handler.getAddress();
        String schedulerName = handler.getSchedulerName();
        String taskName = handler.getTaskName();
        int partitionId = handler.getPartitionId();
        if (address != null) {
            ClientMessage request = ScheduledExecutorGetStatsFromAddressCodec.encodeRequest(schedulerName, taskName, address);
            ClientMessage response = invokeOnAddress(request, address);
            ScheduledExecutorGetStatsFromAddressCodec.ResponseParameters responseParameters =
                    ScheduledExecutorGetStatsFromAddressCodec.decodeResponse(response);
            return new ScheduledTaskStatisticsImpl(responseParameters.totalRuns, responseParameters.lastIdleTimeNanos,
                    responseParameters.totalRunTimeNanos, responseParameters.totalIdleTimeNanos,
                    responseParameters.lastRunDurationNanos);
        } else {
            ClientMessage request = ScheduledExecutorGetStatsFromPartitionCodec.encodeRequest(schedulerName, taskName);
            ClientMessage response = invokeOnPartition(request, partitionId);
            ScheduledExecutorGetStatsFromAddressCodec.ResponseParameters responseParameters =
                    ScheduledExecutorGetStatsFromAddressCodec.decodeResponse(response);
            return new ScheduledTaskStatisticsImpl(responseParameters.totalRuns, responseParameters.lastIdleTimeNanos,
                    responseParameters.totalRunTimeNanos, responseParameters.totalIdleTimeNanos,
                    responseParameters.lastRunDurationNanos);
        }

    }

    @Override
    public long getDelay(TimeUnit unit) {
        checkNotNull(unit, "Unit is null");
        checkAccessibleHandler();
        Address address = handler.getAddress();
        String schedulerName = handler.getSchedulerName();
        String taskName = handler.getTaskName();
        int partitionId = handler.getPartitionId();
        if (address != null) {
            ClientMessage request = ScheduledExecutorGetDelayFromAddressCodec.encodeRequest(schedulerName, taskName, address);
            ClientMessage response = invokeOnAddress(request, address);
            long nanos = ScheduledExecutorGetDelayFromAddressCodec.decodeResponse(response).response;
            return unit.convert(nanos, TimeUnit.NANOSECONDS);
        } else {
            ClientMessage request = ScheduledExecutorGetDelayFromPartitionCodec.encodeRequest(schedulerName, taskName);
            ClientMessage response = invokeOnPartition(request, partitionId);
            long nanos = ScheduledExecutorGetDelayFromPartitionCodec.decodeResponse(response).response;
            return unit.convert(nanos, TimeUnit.NANOSECONDS);
        }
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

        Address address = handler.getAddress();
        String schedulerName = handler.getSchedulerName();
        String taskName = handler.getTaskName();
        int partitionId = handler.getPartitionId();
        if (address != null) {
            ClientMessage request = ScheduledExecutorCancelFromAddressCodec.encodeRequest(schedulerName, taskName,
                    address, false);
            ClientMessage response = invokeOnAddress(request, address);
            return ScheduledExecutorCancelFromAddressCodec.decodeResponse(response).response;
        } else {
            ClientMessage request = ScheduledExecutorCancelFromPartitionCodec.encodeRequest(schedulerName,
                    taskName, false);
            ClientMessage response = invokeOnPartition(request, partitionId);
            return ScheduledExecutorCancelFromPartitionCodec.decodeResponse(response).response;
        }
    }

    @Override
    public boolean isCancelled() {
        checkAccessibleHandler();
        Address address = handler.getAddress();
        String schedulerName = handler.getSchedulerName();
        String taskName = handler.getTaskName();
        int partitionId = handler.getPartitionId();
        if (address != null) {
            ClientMessage request = ScheduledExecutorIsCancelledFromAddressCodec.encodeRequest(schedulerName,
                    taskName, address);
            ClientMessage response = invokeOnAddress(request, address);
            return ScheduledExecutorIsCancelledFromAddressCodec.decodeResponse(response).response;
        } else {
            ClientMessage request = ScheduledExecutorIsCancelledFromPartitionCodec.encodeRequest(schedulerName, taskName);
            ClientMessage response = invokeOnPartition(request, partitionId);
            return ScheduledExecutorIsCancelledFromPartitionCodec.decodeResponse(response).response;
        }
    }

    @Override
    public boolean isDone() {
        checkAccessibleHandler();
        Address address = handler.getAddress();
        String schedulerName = handler.getSchedulerName();
        String taskName = handler.getTaskName();
        int partitionId = handler.getPartitionId();
        if (address != null) {
            ClientMessage request = ScheduledExecutorIsDoneFromAddressCodec.encodeRequest(schedulerName, taskName, address);
            ClientMessage response = invokeOnAddress(request, address);
            return ScheduledExecutorIsDoneFromAddressCodec.decodeResponse(response).response;
        } else {
            ClientMessage request = ScheduledExecutorIsDoneFromPartitionCodec.encodeRequest(schedulerName, taskName);
            ClientMessage response = invokeOnPartition(request, partitionId);
            return ScheduledExecutorIsDoneFromPartitionCodec.decodeResponse(response).response;
        }
    }

    private V get0(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        checkAccessibleHandler();
        Address address = handler.getAddress();
        String schedulerName = handler.getSchedulerName();
        String taskName = handler.getTaskName();
        int partitionId = handler.getPartitionId();
        if (address != null) {
            ClientMessage request = ScheduledExecutorGetResultFromAddressCodec.encodeRequest(schedulerName, taskName, address);
            ClientInvocationFuture future = new ClientInvocation(getClient(), request, schedulerName, address).invoke();
            ClientMessage response = future.get(timeout, unit);
            Data data = ScheduledExecutorGetResultFromAddressCodec.decodeResponse(response).response;
            return toObject(data);
        } else {
            ClientMessage request = ScheduledExecutorGetResultFromPartitionCodec.encodeRequest(schedulerName, taskName);
            ClientInvocationFuture future = new ClientInvocation(getClient(), request, schedulerName, partitionId).invoke();
            ClientMessage response = future.get(timeout, unit);
            Data data = ScheduledExecutorGetResultFromPartitionCodec.decodeResponse(response).response;
            return toObject(data);
        }
    }

    @Override
    public V get()
            throws InterruptedException, ExecutionException {
        try {
            return this.get0(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (TimeoutException e) {
            throw rethrow(e);
        }
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        checkNotNull(unit, "Unit is null");
        return this.get0(timeout, unit);
    }

    public void dispose() {
        checkAccessibleHandler();
        Address address = handler.getAddress();
        String schedulerName = handler.getSchedulerName();
        String taskName = handler.getTaskName();
        int partitionId = handler.getPartitionId();
        if (address != null) {
            ClientMessage request = ScheduledExecutorDisposeFromAddressCodec.encodeRequest(schedulerName, taskName, address);
            invokeOnAddress(request, address);
        } else {
            ClientMessage request = ScheduledExecutorDisposeFromPartitionCodec.encodeRequest(schedulerName, taskName);
            invokeOnPartition(request, partitionId);
        }
        handler = null;

    }

    private void checkAccessibleHandler() {
        if (handler == null) {
            throw new StaleTaskException(
                    "Scheduled task was previously disposed.");
        }
    }

}
