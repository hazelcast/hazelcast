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

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetAllScheduledFuturesCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToPartitionCodec;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.Member;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.scheduledexecutor.NamedTask;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.ScheduledRunnableAdapter;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskHandlerImpl;
import com.hazelcast.scheduledexecutor.impl.TaskDefinition;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.UuidUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.util.FutureUtil.logAllExceptions;
import static com.hazelcast.util.FutureUtil.returnWithDeadline;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Client proxy implementation of {@link IScheduledExecutorService}.
 */
@SuppressWarnings({"unchecked", "checkstyle:methodcount"})
public class ClientScheduledExecutorProxy
        extends PartitionSpecificClientProxy
        implements IScheduledExecutorService {

    private static final int GET_ALL_SCHEDULED_TIMEOUT = 10;

    private static final FutureUtil.ExceptionHandler WHILE_SHUTDOWN_EXCEPTION_HANDLER =
            logAllExceptions("Exception while ScheduledExecutor Service shutdown", Level.FINEST);

    private static final ClientMessageDecoder SUBMIT_DECODER = new ClientMessageDecoder() {
        @Override
        public Void decodeClientMessage(ClientMessage clientMessage) {
            return null;
        }
    };

    private static final ClientMessageDecoder GET_ALL_SCHEDULED_DECODER = new ClientMessageDecoder() {
        @Override
        public List<ScheduledTaskHandler> decodeClientMessage(ClientMessage clientMessage) {
            List<ScheduledTaskHandler> handlers = new ArrayList<ScheduledTaskHandler>();
            List<String> urns = ScheduledExecutorGetAllScheduledFuturesCodec.decodeResponse(clientMessage).handlers;
            for (String urn : urns) {
                handlers.add(ScheduledTaskHandler.of(urn));
            }
            return handlers;
        }
    };

    public ClientScheduledExecutorProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
    }

    @Override
    public String toString() {
        return "ClientScheduledExecutorProxy{" + "name='" + name + '\'' + '}';
    }

    @Override
    public IScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        Callable adapter = createScheduledRunnableAdapter(command);
        return schedule(adapter, delay, unit);
    }

    @Override
    public <V> IScheduledFuture<V> schedule(Callable<V> command, long delay, TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(unit, "Unit is null");

        String name = extractNameOrGenerateOne(command);
        int partitionId = getTaskOrKeyPartitionId(command, name);
        TaskDefinition<V> definition = new TaskDefinition<V>(TaskDefinition.Type.SINGLE_RUN, name, command, delay, unit);
        return scheduleOnPartition(name, definition, partitionId);
    }

    @Override
    public IScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period,
                                                      TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(unit, "Unit is null");

        String name = extractNameOrGenerateOne(command);
        int partitionId = getTaskOrKeyPartitionId(command, name);
        Callable adapter = createScheduledRunnableAdapter(command);
        TaskDefinition definition = new TaskDefinition(TaskDefinition.Type.AT_FIXED_RATE, name, adapter,
                initialDelay, period, unit);

        return scheduleOnPartition(name, definition, partitionId);
    }

    @Override
    public IScheduledFuture<?> scheduleOnMember(Runnable command, Member member, long delay, TimeUnit unit) {
        checkNotNull(member, "Member is null");
        return scheduleOnMembers(command, Collections.singleton(member), delay, unit).get(member);
    }

    @Override
    public <V> IScheduledFuture<V> scheduleOnMember(Callable<V> command, Member member, long delay, TimeUnit unit) {
        checkNotNull(member, "Member is null");
        return scheduleOnMembers(command, Collections.singleton(member), delay, unit).get(member);
    }

    @Override
    public IScheduledFuture<?> scheduleOnMemberAtFixedRate(Runnable command, Member member, long initialDelay,
                                                              long period, TimeUnit unit) {
        checkNotNull(member, "Member is null");
        return scheduleOnMembersAtFixedRate(command, Collections.singleton(member), initialDelay, period, unit).get(member);
    }

    @Override
    public IScheduledFuture<?> scheduleOnKeyOwner(Runnable command, Object key, long delay, TimeUnit unit) {
        Callable adapter = createScheduledRunnableAdapter(command);
        return scheduleOnKeyOwner(adapter, key, delay, unit);
    }

    @Override
    public <V> IScheduledFuture<V> scheduleOnKeyOwner(Callable<V> command, Object key, long delay, TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(key, "Key is null");
        checkNotNull(unit, "Unit is null");

        String name = extractNameOrGenerateOne(command);
        int partitionId = getKeyPartitionId(key);
        TaskDefinition definition = new TaskDefinition(TaskDefinition.Type.SINGLE_RUN, name, command,
                delay, unit);
        return scheduleOnPartition(name, definition, partitionId);
    }

    @Override
    public IScheduledFuture<?> scheduleOnKeyOwnerAtFixedRate(Runnable command, Object key, long initialDelay,
                                                                long period, TimeUnit unit) {
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

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnAllMembers(Runnable command, long delay, TimeUnit unit) {
        return scheduleOnMembers(command, getContext().getClusterService().getMemberList(), delay, unit);
    }

    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnAllMembers(Callable<V> command, long delay,
                                                                     TimeUnit unit) {
        return scheduleOnMembers(command, getContext().getClusterService().getMemberList(), delay, unit);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnAllMembersAtFixedRate(Runnable command, long initialDelay,
                                                                               long period, TimeUnit unit) {
        return scheduleOnMembersAtFixedRate(command, getContext().getClusterService().getMemberList(),
                initialDelay, period, unit);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnMembers(Runnable command, Collection<Member> members, long delay,
                                                              TimeUnit unit) {
        Callable adapter = createScheduledRunnableAdapter(command);
        return scheduleOnMembers(adapter, members, delay, unit);
    }

    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnMembers(Callable<V> command, Collection<Member> members,
                                                                  long delay, TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(members, "Members is null");
        checkNotNull(unit, "Unit is null");

        String name = extractNameOrGenerateOne(command);
        Map<Member, IScheduledFuture<V>> futures = new HashMap<Member, IScheduledFuture<V>>();
        for (Member member : members) {
            TaskDefinition definition = new TaskDefinition(
                    TaskDefinition.Type.SINGLE_RUN, name, command, delay, unit);

            futures.put(member, (IScheduledFuture<V>) scheduleOnMember(name, member, definition));
        }

        return futures;
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnMembersAtFixedRate(Runnable command,
                                                                            Collection<Member> members, long initialDelay,
                                                                            long period, TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(members, "Members is null");
        checkNotNull(unit, "Unit is null");

        String name = extractNameOrGenerateOne(command);
        Callable adapter = createScheduledRunnableAdapter(command);
         Map<Member, IScheduledFuture<?>> futures = new HashMap<Member, IScheduledFuture<?>>();
        for (Member member : members) {
            TaskDefinition definition = new TaskDefinition(
                    TaskDefinition.Type.AT_FIXED_RATE, name, adapter, initialDelay, period, unit);

            futures.put(member, scheduleOnMember(name, member, definition));
        }

        return futures;
    }

    @Override
    public <V> IScheduledFuture<V> getScheduledFuture(ScheduledTaskHandler handler) {
        ClientScheduledFutureProxy<V> futureProxy = new ClientScheduledFutureProxy<V>(handler, getContext());
        return futureProxy;
    }

    @Override
    public <V> Map<Member, List<IScheduledFuture<V>>> getAllScheduledFutures() {
        final long timeout = GET_ALL_SCHEDULED_TIMEOUT;
        Map<Member, List<IScheduledFuture<V>>> tasks =
                new LinkedHashMap<Member, List<IScheduledFuture<V>>>();

        List<Member> members = new ArrayList<Member>(getContext().getClusterService().getMemberList());
        List<Future<List<ScheduledTaskHandler>>> calls = new ArrayList<Future<List<ScheduledTaskHandler>>>();
        for (Member member : members) {
            Address address = member.getAddress();
            ClientMessage request = ScheduledExecutorGetAllScheduledFuturesCodec.encodeRequest(getName(), address);

            calls.add(ClientScheduledExecutorProxy.this.<List<ScheduledTaskHandler>>
                    doSubmitOnAddress(request, GET_ALL_SCHEDULED_DECODER, address));
        }

        List<List<ScheduledTaskHandler>> resolvedFutures = new ArrayList<List<ScheduledTaskHandler>>(
                returnWithDeadline(calls, timeout, TimeUnit.SECONDS));

        for (int i = 0; i < resolvedFutures.size(); i++) {
            Member member = members.get(i);
            List<ScheduledTaskHandler> handlers = resolvedFutures.get(i);
            List<IScheduledFuture<V>> scheduledFutures = new ArrayList<IScheduledFuture<V>>();

            for (ScheduledTaskHandler handler : handlers) {
                scheduledFutures.add(this.<V>createFutureProxy(handler));
            }

            if (!scheduledFutures.isEmpty()) {
                tasks.put(member, scheduledFutures);
            }
        }
        return tasks;
    }

    @Override
    public void shutdown() {
        Collection<Member> members = getContext().getClusterService().getMemberList();
        Collection<Future> calls = new LinkedList<Future>();

        for (Member member : members) {
            ClientMessage request = ScheduledExecutorShutdownCodec.encodeRequest(getName(), member.getAddress());
            calls.add(doSubmitOnAddress(request, SUBMIT_DECODER, member.getAddress()));
        }

        waitWithDeadline(calls, 1, TimeUnit.SECONDS, WHILE_SHUTDOWN_EXCEPTION_HANDLER);
    }

    private <T> ScheduledRunnableAdapter<T> createScheduledRunnableAdapter(Runnable command) {
        checkNotNull(command, "Command can't be null");

        return new ScheduledRunnableAdapter<T>(command);
    }

    private <V> IScheduledFuture<V> createFutureProxy(ScheduledTaskHandler handler) {
        ClientScheduledFutureProxy<V> proxy = new ClientScheduledFutureProxy<V>(handler, getContext());
        return proxy;
    }

    private <V> IScheduledFuture<V> createFutureProxy(int partitionId, String taskName) {
        return createFutureProxy(ScheduledTaskHandlerImpl.of(partitionId, getName(), taskName));
    }

    private <V> IScheduledFuture<V> createFutureProxy(Address address, String taskName) {
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

    private <V> IScheduledFuture<V> scheduleOnPartition(String name, TaskDefinition definition, int partitionId) {
        Data data = getSerializationService().toData(definition);
        ClientMessage request = ScheduledExecutorSubmitToPartitionCodec.encodeRequest(getName(), data);
        return scheduleOnPartition(name, request, SUBMIT_DECODER, partitionId);
    }

    private <V> IScheduledFuture<V> scheduleOnPartition(String name, ClientMessage clientMessage,
                                                        ClientMessageDecoder clientMessageDecoder,
                                                        int partitionId) {
        clientMessage.setPartitionId(partitionId);
        doSubmitOnPartition(clientMessage, clientMessageDecoder, partitionId).join();
        return createFutureProxy(partitionId, name);
    }

    private <V> IScheduledFuture<V> scheduleOnMember(String name, Member member, TaskDefinition definition) {
        Data data = getSerializationService().toData(definition);
        ClientMessage request = ScheduledExecutorSubmitToAddressCodec.encodeRequest(getName(), member.getAddress(), data);
        doSubmitOnAddress(request, SUBMIT_DECODER, member.getAddress()).join();
        return createFutureProxy(member.getAddress(), name);
    }

    private <T> ClientDelegatingFuture<T> doSubmitOnPartition(ClientMessage clientMessage,
                                                    ClientMessageDecoder clientMessageDecoder,
                                                    int partitionId) {
        SerializationService serializationService = getContext().getSerializationService();

        try {
            final ClientInvocationFuture future = new ClientInvocation(getClient(), clientMessage,
                    partitionId).invoke();

            return new ClientDelegatingFuture<T>(future, serializationService, clientMessageDecoder);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private <T> ClientDelegatingFuture<T> doSubmitOnAddress(ClientMessage clientMessage,
                                                              ClientMessageDecoder clientMessageDecoder,
                                                              Address address) {
        SerializationService serializationService = getContext().getSerializationService();

        try {
            final ClientInvocationFuture future = new ClientInvocation(getClient(), clientMessage,
                    address).invoke();

            return new ClientDelegatingFuture<T>(future, serializationService, clientMessageDecoder);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

}
