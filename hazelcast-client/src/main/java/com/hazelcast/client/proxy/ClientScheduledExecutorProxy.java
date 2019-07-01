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

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetAllScheduledFuturesCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToPartitionCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.cluster.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.scheduledexecutor.NamedTask;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.ScheduledRunnableAdapter;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskHandlerImpl;
import com.hazelcast.scheduledexecutor.impl.TaskDefinition;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.UuidUtil;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Client proxy implementation of {@link IScheduledExecutorService}.
 */
@SuppressWarnings({"unchecked", "checkstyle:methodcount"})
public class ClientScheduledExecutorProxy
        extends PartitionSpecificClientProxy
        implements IScheduledExecutorService {

    private static final int SHUTDOWN_TIMEOUT = 10;

    private static final ILogger LOGGER = Logger.getLogger(ClientScheduledExecutorProxy.class);

    private static final ClientMessageDecoder SUBMIT_DECODER = clientMessage -> null;

    private final FutureUtil.ExceptionHandler shutdownExceptionHandler = throwable -> {
        if (throwable != null) {
            if (throwable instanceof QuorumException) {
                sneakyThrow(throwable);
            }
            if (throwable.getCause() instanceof QuorumException) {
                sneakyThrow(throwable.getCause());
            }
        }
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "Exception while ExecutorService shutdown", throwable);
        }
    };

    public ClientScheduledExecutorProxy(String serviceName, String objectId, ClientContext context) {
        super(serviceName, objectId, context);
    }

    @Override
    public String toString() {
        return "ClientScheduledExecutorProxy{" + "name='" + name + '\'' + '}';
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> schedule(@Nonnull Runnable command, long delay, @Nonnull TimeUnit unit) {
        Callable adapter = createScheduledRunnableAdapter(command);
        return schedule(adapter, delay, unit);
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> schedule(@Nonnull Callable<V> command, long delay, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(unit, "Unit is null");

        String name = extractNameOrGenerateOne(command);
        int partitionId = getTaskOrKeyPartitionId(command, name);
        TaskDefinition<V> definition = new TaskDefinition<>(TaskDefinition.Type.SINGLE_RUN, name, command, delay, unit);
        return scheduleOnPartition(name, definition, partitionId);
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> scheduleAtFixedRate(@Nonnull Runnable command, long initialDelay, long period,
                                                       @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(unit, "Unit is null");

        String name = extractNameOrGenerateOne(command);
        int partitionId = getTaskOrKeyPartitionId(command, name);
        Callable adapter = createScheduledRunnableAdapter(command);
        TaskDefinition definition = new TaskDefinition(TaskDefinition.Type.AT_FIXED_RATE, name, adapter,
                initialDelay, period, unit);

        return scheduleOnPartition(name, definition, partitionId);
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> scheduleOnMember(@Nonnull Runnable command,
                                                    @Nonnull Member member,
                                                    long delay, @Nonnull TimeUnit unit) {
        checkNotNull(member, "Member is null");
        Map<Member, IScheduledFuture<V>> futureMap =
                scheduleOnMembers(command, Collections.singleton(member), delay, unit);
        return futureMap.get(member);
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> scheduleOnMember(@Nonnull Callable<V> command,
                                                    @Nonnull Member member,
                                                    long delay, @Nonnull TimeUnit unit) {
        checkNotNull(member, "Member is null");
        return scheduleOnMembers(command, Collections.singleton(member), delay, unit).get(member);
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> scheduleOnMemberAtFixedRate(@Nonnull Runnable command,
                                                               @Nonnull Member member,
                                                               long initialDelay, long period, @Nonnull TimeUnit unit) {
        checkNotNull(member, "Member is null");
        Map<Member, IScheduledFuture<V>> futureMap =
                scheduleOnMembersAtFixedRate(command, Collections.singleton(member), initialDelay, period, unit);
        return futureMap.get(member);
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> scheduleOnKeyOwner(@Nonnull Runnable command,
                                                      @Nonnull Object key,
                                                      long delay, @Nonnull TimeUnit unit) {
        Callable adapter = createScheduledRunnableAdapter(command);
        return scheduleOnKeyOwner(adapter, key, delay, unit);
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> scheduleOnKeyOwner(@Nonnull Callable<V> command,
                                                      @Nonnull Object key,
                                                      long delay, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(key, "Key is null");
        checkNotNull(unit, "Unit is null");

        String name = extractNameOrGenerateOne(command);
        int partitionId = getKeyPartitionId(key);
        TaskDefinition definition = new TaskDefinition(TaskDefinition.Type.SINGLE_RUN, name, command,
                delay, unit);
        return scheduleOnPartition(name, definition, partitionId);
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> scheduleOnKeyOwnerAtFixedRate(@Nonnull Runnable command,
                                                                 @Nonnull Object key,
                                                                 long initialDelay, long period, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(key, "Key is null");
        checkNotNull(unit, "Unit is null");

        String name = extractNameOrGenerateOne(command);
        int partitionId = getKeyPartitionId(key);
        Callable adapter = createScheduledRunnableAdapter(command);
        TaskDefinition definition = new TaskDefinition(TaskDefinition.Type.AT_FIXED_RATE, name, adapter,
                initialDelay, period, unit);
        return scheduleOnPartition(name, definition, partitionId);
    }

    @Nonnull
    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnAllMembers(@Nonnull Runnable command,
                                                                     long delay, @Nonnull TimeUnit unit) {
        return scheduleOnMembers(command, getContext().getClusterService().getMemberList(), delay, unit);
    }

    @Nonnull
    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnAllMembers(@Nonnull Callable<V> command, long delay,
                                                                     @Nonnull TimeUnit unit) {
        return scheduleOnMembers(command, getContext().getClusterService().getMemberList(), delay, unit);
    }

    @Nonnull
    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnAllMembersAtFixedRate(@Nonnull Runnable command,
                                                                                long initialDelay,
                                                                                long period, @Nonnull TimeUnit unit) {
        return scheduleOnMembersAtFixedRate(command, getContext().getClusterService().getMemberList(),
                initialDelay, period, unit);
    }

    @Nonnull
    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnMembers(@Nonnull Runnable command,
                                                                  @Nonnull Collection<Member> members,
                                                                  long delay, @Nonnull TimeUnit unit) {
        Callable adapter = createScheduledRunnableAdapter(command);
        return scheduleOnMembers(adapter, members, delay, unit);
    }

    @Nonnull
    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnMembers(@Nonnull Callable<V> command,
                                                                  @Nonnull Collection<Member> members,
                                                                  long delay, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(members, "Members is null");
        checkNotNull(unit, "Unit is null");

        String name = extractNameOrGenerateOne(command);
        Map<Member, IScheduledFuture<V>> futures = new HashMap<>();
        for (Member member : members) {
            TaskDefinition definition = new TaskDefinition(
                    TaskDefinition.Type.SINGLE_RUN, name, command, delay, unit);

            futures.put(member, scheduleOnMember(name, member, definition));
        }

        return futures;
    }

    @Nonnull
    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnMembersAtFixedRate(@Nonnull Runnable command,
                                                                             @Nonnull Collection<Member> members,
                                                                             long initialDelay,
                                                                             long period, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(members, "Members is null");
        checkNotNull(unit, "Unit is null");

        String name = extractNameOrGenerateOne(command);
        Callable adapter = createScheduledRunnableAdapter(command);
        Map<Member, IScheduledFuture<V>> futures = new HashMap<>();
        for (Member member : members) {
            TaskDefinition definition = new TaskDefinition(
                    TaskDefinition.Type.AT_FIXED_RATE, name, adapter, initialDelay, period, unit);

            futures.put(member, scheduleOnMember(name, member, definition));
        }

        return futures;
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> getScheduledFuture(@Nonnull ScheduledTaskHandler handler) {
        checkNotNull(handler, "Handler is null");

        return new ClientScheduledFutureProxy<>(handler, getContext());
    }

    @Nonnull
    @Override
    public <V> Map<Member, List<IScheduledFuture<V>>> getAllScheduledFutures() {
        ClientMessage request = ScheduledExecutorGetAllScheduledFuturesCodec.encodeRequest(getName());
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, getName()).invoke();
        ClientMessage response;
        try {
            response = future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }

        Collection<Map.Entry<Member, List<ScheduledTaskHandler>>> urnsPerMember =
                ScheduledExecutorGetAllScheduledFuturesCodec.decodeResponse(response).handlers;

        Map<Member, List<IScheduledFuture<V>>> tasksMap = new HashMap<>();

        for (Map.Entry<Member, List<ScheduledTaskHandler>> entry : urnsPerMember) {
            List<IScheduledFuture<V>> memberTasks = new ArrayList<>();
            for (ScheduledTaskHandler scheduledTaskHandler : entry.getValue()) {
                memberTasks.add(new ClientScheduledFutureProxy(scheduledTaskHandler, getContext()));
            }

            tasksMap.put(entry.getKey(), memberTasks);
        }

        return tasksMap;
    }

    @Override
    public void shutdown() {
        Collection<Member> members = getContext().getClusterService().getMemberList();
        Collection<Future> calls = new LinkedList<>();

        for (Member member : members) {
            ClientMessage request = ScheduledExecutorShutdownCodec.encodeRequest(getName(), member.getAddress());
            calls.add(doSubmitOnAddress(request, SUBMIT_DECODER, member.getAddress()));
        }

        waitWithDeadline(calls, SHUTDOWN_TIMEOUT, TimeUnit.SECONDS, shutdownExceptionHandler);
    }

    private <T> ScheduledRunnableAdapter<T> createScheduledRunnableAdapter(Runnable command) {
        checkNotNull(command, "Command is null");

        return new ScheduledRunnableAdapter<>(command);
    }

    private @Nonnull
    <V> IScheduledFuture<V> createFutureProxy(ScheduledTaskHandler handler) {
        return new ClientScheduledFutureProxy<>(handler, getContext());
    }

    private @Nonnull
    <V> IScheduledFuture<V> createFutureProxy(int partitionId, String taskName) {
        return createFutureProxy(ScheduledTaskHandlerImpl.of(partitionId, getName(), taskName));
    }

    private @Nonnull
    <V> IScheduledFuture<V> createFutureProxy(Address address, String taskName) {
        return createFutureProxy(ScheduledTaskHandlerImpl.of(address, getName(), taskName));
    }

    private int getKeyPartitionId(Object key) {
        return getClient().getPartitionService().getPartition(key).getPartitionId();
    }

    private int getTaskOrKeyPartitionId(Callable task, Object key) {
        if (task instanceof PartitionAware) {
            Object newKey = ((PartitionAware) task).getPartitionKey();
            if (newKey != null) {
                key = newKey;
            }
        }

        return getKeyPartitionId(key);
    }

    private int getTaskOrKeyPartitionId(Runnable task, Object key) {
        if (task instanceof PartitionAware) {
            Object newKey = ((PartitionAware) task).getPartitionKey();
            if (newKey != null) {
                key = newKey;
            }
        }

        return getKeyPartitionId(key);
    }

    private String extractNameOrGenerateOne(Object command) {
        String name = null;
        if (command instanceof NamedTask) {
            name = ((NamedTask) command).getName();
        }

        return name != null ? name : UuidUtil.newUnsecureUuidString();
    }

    private @Nonnull
    <V> IScheduledFuture<V> scheduleOnPartition(String name, TaskDefinition definition, int partitionId) {
        TimeUnit unit = definition.getUnit();
        Data commandData = getSerializationService().toData(definition.getCommand());
        ClientMessage request = ScheduledExecutorSubmitToPartitionCodec.encodeRequest(getName(),
                definition.getType().getId(), definition.getName(), commandData,
                unit.toMillis(definition.getInitialDelay()),
                unit.toMillis(definition.getPeriod()));
        try {
            new ClientInvocation(getClient(), request, getName(), partitionId).invoke().get();
        } catch (Exception e) {
            throw rethrow(e);
        }
        return createFutureProxy(partitionId, name);
    }

    private @Nonnull
    <V> IScheduledFuture<V> scheduleOnMember(String name, Member member, TaskDefinition definition) {
        TimeUnit unit = definition.getUnit();

        Data commandData = getSerializationService().toData(definition.getCommand());
        ClientMessage request = ScheduledExecutorSubmitToAddressCodec.encodeRequest(getName(), member.getAddress(),
                definition.getType().getId(), definition.getName(), commandData,
                unit.toMillis(definition.getInitialDelay()),
                unit.toMillis(definition.getPeriod()));
        try {
            new ClientInvocation(getClient(), request, getName(), member.getAddress()).invoke().get();
        } catch (Exception e) {
            throw rethrow(e);
        }
        return createFutureProxy(member.getAddress(), name);
    }

    private <T> ClientDelegatingFuture<T> doSubmitOnAddress(ClientMessage clientMessage,
                                                            ClientMessageDecoder clientMessageDecoder,
                                                            Address address) {
        try {
            ClientInvocationFuture future = new ClientInvocation(getClient(), clientMessage, getName(), address).invoke();
            return new ClientDelegatingFuture<T>(future, getSerializationService(), clientMessageDecoder);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
}
