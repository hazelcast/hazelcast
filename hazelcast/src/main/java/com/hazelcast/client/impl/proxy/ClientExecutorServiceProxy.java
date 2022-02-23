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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceIsShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToMemberCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToPartitionCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.executor.LocalExecutorStats;
import com.hazelcast.executor.impl.ExecutionCallbackAdapter;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;

/**
 * @author ali 5/24/13
 */
public class ClientExecutorServiceProxy extends ClientProxy implements IExecutorService {

    private final Random random = new Random(-System.currentTimeMillis());

    public ClientExecutorServiceProxy(String serviceName, String objectId, ClientContext context) {
        super(serviceName, objectId, context);
    }

    // execute on members

    @Override
    public void execute(@Nonnull Runnable command) {
        submit(command);
    }

    @Override
    public void executeOnKeyOwner(@Nonnull Runnable command,
                                  @Nonnull Object key) {
        submitToKeyOwnerInternal(toData(command), key, null);
    }

    @Override
    public void executeOnMember(@Nonnull Runnable command,
                                @Nonnull Member member) {
        checkNotNull(member, "member must not be null");
        submitToTargetInternal(toData(command), member, null);
    }

    @Override
    public void executeOnMembers(@Nonnull Runnable command,
                                 @Nonnull Collection<Member> members) {
        checkNotNull(members, "members must not be null");
        for (Member member : members) {
            executeOnMember(command, member);
        }
    }

    @Override
    public void execute(@Nonnull Runnable command,
                        @Nonnull MemberSelector memberSelector) {
        List<Member> members = selectMembers(memberSelector);
        int selectedMember = random.nextInt(members.size());
        executeOnMember(command, members.get(selectedMember));
    }

    @Override
    public void executeOnMembers(@Nonnull Runnable command,
                                 @Nonnull MemberSelector memberSelector) {
        List<Member> members = selectMembers(memberSelector);
        executeOnMembers(command, members);
    }

    @Override
    public void executeOnAllMembers(@Nonnull Runnable command) {
        final Collection<Member> memberList = getContext().getClusterService().getMemberList();
        for (Member member : memberList) {
            executeOnMember(command, member);
        }
    }


    // submit to members

    @Override
    public <T> Future<T> submitToMember(@Nonnull Callable<T> task,
                                        @Nonnull Member member) {
        checkNotNull(member, "member must not be null");
        return submitToTargetInternal(toData(task), member, (T) null);
    }

    @Override
    public <T> Map<Member, Future<T>> submitToMembers(@Nonnull Callable<T> task,
                                                      @Nonnull Collection<Member> members) {
        checkNotNull(members, "members must not be null");
        Map<Member, Future<T>> futureMap = new HashMap<>(members.size());
        Data taskData = toData(task);
        for (Member member : members) {
            Future<T> f = submitToTargetInternal(taskData, member, (T) null);
            futureMap.put(member, f);
        }
        return futureMap;
    }

    @Override
    public <T> Future<T> submit(@Nonnull Callable<T> task,
                                @Nonnull MemberSelector memberSelector) {
        List<Member> members = selectMembers(memberSelector);
        int selectedMember = random.nextInt(members.size());
        return submitToMember(task, members.get(selectedMember));
    }

    @Override
    public <T> Map<Member, Future<T>> submitToMembers(@Nonnull Callable<T> task,
                                                      @Nonnull MemberSelector memberSelector) {
        List<Member> members = selectMembers(memberSelector);
        return submitToMembers(task, members);
    }

    @Override
    public <T> Map<Member, Future<T>> submitToAllMembers(@Nonnull Callable<T> task) {
        final Collection<Member> memberList = getContext().getClusterService().getMemberList();
        Map<Member, Future<T>> futureMap = new HashMap<>(memberList.size());
        Data taskData = toData(task);
        for (Member m : memberList) {
            Future<T> f = submitToTargetInternal(taskData, m, (T) null);
            futureMap.put(m, f);
        }
        return futureMap;
    }

    // submit to members callback
    @Override
    public void submitToMember(@Nonnull Runnable command,
                               @Nonnull Member member,
                               @Nullable ExecutionCallback callback) {
        checkNotNull(member, "member must not be null");
        submitToTargetInternal(toData(command), member, callback);
    }

    @Override
    public void submitToMembers(@Nonnull Runnable command,
                                @Nonnull Collection<Member> members,
                                @Nonnull MultiExecutionCallback callback) {
        checkNotNull(members, "members must not be null");
        MultiExecutionCallbackWrapper multiExecutionCallbackWrapper =
                new MultiExecutionCallbackWrapper(members.size(), callback);
        for (Member member : members) {
            final ExecutionCallbackWrapper executionCallback =
                    new ExecutionCallbackWrapper(multiExecutionCallbackWrapper, member);
            submitToMember(command, member, executionCallback);
        }
    }

    @Override
    public <T> void submitToMember(@Nonnull Callable<T> task,
                                   @Nonnull Member member,
                                   @Nullable ExecutionCallback<T> callback) {
        checkNotNull(member, "member must not be null");
        submitToTargetInternal(toData(task), member, callback);
    }

    @Override
    public <T> void submitToMembers(@Nonnull Callable<T> task,
                                    @Nonnull Collection<Member> members,
                                    @Nonnull MultiExecutionCallback callback) {
        checkNotNull(members, "members must not be null");
        MultiExecutionCallbackWrapper multiExecutionCallbackWrapper =
                new MultiExecutionCallbackWrapper(members.size(), callback);
        Data taskData = toData(task);
        members.forEach(member -> {
            final ExecutionCallbackWrapper<T> executionCallback =
                    new ExecutionCallbackWrapper<>(multiExecutionCallbackWrapper, member);
            submitToTargetInternal(taskData, member, executionCallback);
        });
    }

    @Override
    public void submit(@Nonnull Runnable task,
                       @Nonnull MemberSelector memberSelector,
                       @Nullable ExecutionCallback callback) {
        List<Member> members = selectMembers(memberSelector);
        int selectedMember = random.nextInt(members.size());
        submitToMember(task, members.get(selectedMember), callback);
    }

    @Override
    public void submitToMembers(@Nonnull Runnable task,
                                @Nonnull MemberSelector memberSelector,
                                @Nonnull MultiExecutionCallback callback) {
        List<Member> members = selectMembers(memberSelector);
        submitToMembers(task, members, callback);
    }

    @Override
    public <T> void submit(@Nonnull Callable<T> task,
                           @Nonnull MemberSelector memberSelector,
                           @Nullable ExecutionCallback<T> callback) {
        List<Member> members = selectMembers(memberSelector);
        int selectedMember = random.nextInt(members.size());
        submitToMember(task, members.get(selectedMember), callback);
    }

    @Override
    public <T> void submitToMembers(@Nonnull Callable<T> task,
                                    @Nonnull MemberSelector memberSelector,
                                    @Nonnull MultiExecutionCallback callback) {
        List<Member> members = selectMembers(memberSelector);
        submitToMembers(task, members, callback);
    }

    @Override
    public void submitToAllMembers(@Nonnull Runnable command,
                                   @Nonnull MultiExecutionCallback callback) {
        final Collection<Member> memberList = getContext().getClusterService().getMemberList();
        submitToMembers(command, memberList, callback);
    }

    @Override
    public <T> void submitToAllMembers(@Nonnull Callable<T> task,
                                       @Nonnull MultiExecutionCallback callback) {
        final Collection<Member> memberList = getContext().getClusterService().getMemberList();
        submitToMembers(task, memberList, callback);
    }

    // submit random

    @Nonnull
    @Override
    public Future<?> submit(@Nonnull Runnable command) {
        final Object partitionKey = getTaskPartitionKey(command);
        Data taskData = toData(command);
        if (partitionKey != null) {
            return submitToKeyOwnerInternal(taskData, partitionKey, null);
        }
        return submitToRandomInternal(taskData, null);
    }

    @Nonnull
    @Override
    public <T> Future<T> submit(@Nonnull Runnable command, T result) {
        final Object partitionKey = getTaskPartitionKey(command);
        Data taskData = toData(command);
        if (partitionKey != null) {
            return submitToKeyOwnerInternal(taskData, partitionKey, result);
        }
        return submitToRandomInternal(taskData, result);
    }

    @Nonnull
    @Override
    public <T> Future<T> submit(@Nonnull Callable<T> task) {
        final Object partitionKey = getTaskPartitionKey(task);
        if (partitionKey != null) {
            return submitToKeyOwner(task, partitionKey);
        }
        return submitToRandomInternal(toData(task), null);
    }

    @Override
    public <T> void submit(@Nonnull Runnable command,
                           @Nullable ExecutionCallback<T> callback) {
        final Object partitionKey = getTaskPartitionKey(command);
        Data task = toData(command);
        if (partitionKey != null) {
            submitToKeyOwnerInternal(task, partitionKey, callback);
        } else {
            submitToRandomWithCallbackInternal(task, callback);
        }
    }

    @Override
    public <T> void submit(@Nonnull Callable<T> task, @Nullable ExecutionCallback<T> callback) {
        final Object partitionKey = getTaskPartitionKey(task);
        Data taskData = toData(task);
        if (partitionKey != null) {
            submitToKeyOwnerInternal(taskData, partitionKey, callback);
        } else {
            submitToRandomWithCallbackInternal(taskData, callback);
        }
    }

    // submit to key

    @Override
    public <T> Future<T> submitToKeyOwner(@Nonnull Callable<T> task,
                                          @Nonnull Object key) {
        return submitToKeyOwnerInternal(toData(task), key, null);
    }

    @Override
    public void submitToKeyOwner(@Nonnull Runnable command,
                                 @Nonnull Object key,
                                 @Nonnull ExecutionCallback callback) {
        submitToKeyOwnerInternal(toData(command), key, callback);
    }

    @Override
    public <T> void submitToKeyOwner(@Nonnull Callable<T> task,
                                     @Nonnull Object key,
                                     @Nullable ExecutionCallback<T> callback) {
        submitToKeyOwnerInternal(toData(task), key, callback);
    }

    // end

    @Override
    public LocalExecutorStats getLocalExecutorStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public void shutdown() {
        ClientMessage request = ExecutorServiceShutdownCodec.encodeRequest(name);
        invoke(request);
    }

    @Nonnull
    @Override
    public List<Runnable> shutdownNow() {
        shutdown();
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        ClientMessage request = ExecutorServiceIsShutdownCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        return ExecutorServiceIsShutdownCodec.decodeResponse(response);
    }

    @Override
    public boolean isTerminated() {
        return isShutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) {
        checkNotNull(unit, "unit must not be null");
        return false;
    }

    @Nonnull
    @Override
    public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks) {
        checkNotNull(tasks, "tasks must not be null");
        final List<Future<T>> futures = new ArrayList<>(tasks.size());
        final List<Future<T>> result = new ArrayList<>(tasks.size());
        for (Callable<T> task : tasks) {
            futures.add(submitToRandomInternal(toData(task), null));
        }
        for (Future<T> future : futures) {
            Object value = retrieveResult(future);
            result.add(newCompletedFuture(value, getSerializationService()));
        }
        return result;
    }

    @Nonnull
    @Override
    public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks,
                                         long timeout, @Nonnull TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks,
                           long timeout, @Nonnull TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    private Object getTaskPartitionKey(Object task) {
        if (task instanceof PartitionAware) {
            return ((PartitionAware) task).getPartitionKey();
        }
        return null;
    }

    private @Nonnull
    <T> Future<T> submitToKeyOwnerInternal(@Nonnull Data task,
                                           @Nonnull Object key,
                                           T defaultValue) {
        checkNotNull(task, "task should not be null");
        checkNotNull(key, "key should not be null");

        UUID uuid = getUUID();
        int partitionId = getPartitionId(key);
        ClientMessage request = ExecutorServiceSubmitToPartitionCodec.encodeRequest(name, uuid, task);
        ClientInvocationFuture f = invokeOnPartitionOwner(request, partitionId);
        return delegatingFuture(f, uuid, partitionId, defaultValue);
    }

    private <T> Future<T> submitToKeyOwnerInternal(@Nonnull Data task,
                                                   @Nonnull Object key,
                                                   @Nullable ExecutionCallback<T> callback) {
        checkNotNull(task, "task should not be null");
        checkNotNull(key, "key should not be null");
        UUID uuid = getUUID();
        int partitionId = getPartitionId(key);
        ClientMessage request = ExecutorServiceSubmitToPartitionCodec.encodeRequest(name, uuid, task);
        ClientInvocationFuture f = invokeOnPartitionOwner(request, partitionId);

        InternalCompletableFuture<T> delegatingFuture = (InternalCompletableFuture<T>) delegatingFuture(f, uuid, partitionId,
                (T) null);

        if (callback != null) {
            delegatingFuture.whenCompleteAsync(new ExecutionCallbackAdapter<>(callback))
                    .whenCompleteAsync((v, t) -> {
                        if (t instanceof RejectedExecutionException) {
                            callback.onFailure(t);
                        }
                    }, ConcurrencyUtil.getDefaultAsyncExecutor());
        }
        return delegatingFuture;
    }

    private @Nonnull
    <T> Future<T> submitToRandomInternal(Data task, T defaultValue) {
        checkNotNull(task, "task should not be null");

        UUID uuid = getUUID();
        int partitionId = randomPartitionId();
        ClientMessage request = ExecutorServiceSubmitToPartitionCodec.encodeRequest(name, uuid, task);
        ClientInvocationFuture f = invokeOnPartitionOwner(request, partitionId);
        return delegatingFuture(f, uuid, partitionId, defaultValue);
    }

    private <T> void submitToRandomWithCallbackInternal(Data task, ExecutionCallback<T> callback) {
        checkNotNull(task, "task should not be null");

        UUID uuid = getUUID();
        int partitionId = randomPartitionId();
        ClientMessage request = ExecutorServiceSubmitToPartitionCodec.encodeRequest(name, uuid, task);
        ClientInvocationFuture f = invokeOnPartitionOwner(request, partitionId);
        InternalCompletableFuture<T> delegatingFuture = (InternalCompletableFuture<T>) delegatingFuture(f, uuid, partitionId,
                (T) null);
        if (callback != null) {
            delegatingFuture.whenCompleteAsync(new ExecutionCallbackAdapter<>(callback))
                    .whenCompleteAsync((v, t) -> {
                        if (t instanceof RejectedExecutionException) {
                            callback.onFailure(t);
                        }
                    }, ConcurrencyUtil.getDefaultAsyncExecutor());
        }
    }

    private <T> Future<T> submitToTargetInternal(@Nonnull Data task,
                                                 Member member,
                                                 T defaultValue) {
        checkNotNull(task, "task should not be null");

        UUID uuid = getUUID();
        ClientMessage request = ExecutorServiceSubmitToMemberCodec.encodeRequest(name, uuid, task, member.getUuid());
        ClientInvocationFuture f = invokeOnTarget(request, member);
        return delegatingFuture(f, uuid, member, defaultValue);
    }

    private <T> void submitToTargetInternal(@Nonnull Data task,
                                            Member member,
                                            @Nullable ExecutionCallback<T> callback) {
        checkNotNull(task, "task should not be null");

        UUID uuid = getUUID();
        ClientMessage request = ExecutorServiceSubmitToMemberCodec.encodeRequest(name, uuid, task, member.getUuid());
        ClientInvocationFuture f = invokeOnTarget(request, member);
        InternalCompletableFuture<T> delegatingFuture = (InternalCompletableFuture<T>) delegatingFuture(f, uuid, member,
                (T) null);
        if (callback != null) {
            delegatingFuture.whenCompleteAsync(new ExecutionCallbackAdapter<>(callback))
                    .whenCompleteAsync((v, t) -> {
                        if (t instanceof RejectedExecutionException) {
                            callback.onFailure(t);
                        }
                    }, ConcurrencyUtil.getDefaultAsyncExecutor());
        }
    }

    @Override
    public String toString() {
        return "IExecutorService{" + "name='" + name + '\'' + '}';
    }

    private <T> Future<T> delegatingFuture(ClientInvocationFuture f,
                                           UUID uuid,
                                           Member member,
                                           T defaultValue) {
        return new IExecutorDelegatingFuture<>(f, getContext(), uuid, defaultValue,
                ExecutorServiceSubmitToMemberCodec::decodeResponse, name, member);
    }

    private @Nonnull
    <T> Future<T> delegatingFuture(ClientInvocationFuture f, UUID uuid, int partitionId, T defaultValue) {
        return new IExecutorDelegatingFuture<>(f, getContext(), uuid, defaultValue,
                ExecutorServiceSubmitToPartitionCodec::decodeResponse, name, partitionId);
    }

    private <T> Object retrieveResult(Future<T> f) {
        Object response;
        try {
            response = f.get();
        } catch (Exception e) {
            response = e;
        }
        return response;
    }

    private List<Member> selectMembers(MemberSelector memberSelector) {
        checkNotNull(memberSelector, "memberSelector must not be null");
        List<Member> selected = new ArrayList<>();
        Collection<Member> members = getContext().getClusterService().getMemberList();
        for (Member member : members) {
            if (memberSelector.select(member)) {
                selected.add(member);
            }
        }
        if (selected.isEmpty()) {
            throw new RejectedExecutionException("No member selected with memberSelector[" + memberSelector + "]");
        }
        return selected;
    }

    private static final class ExecutionCallbackWrapper<T> implements ExecutionCallback<T> {
        MultiExecutionCallbackWrapper multiExecutionCallbackWrapper;

        Member member;

        private ExecutionCallbackWrapper(MultiExecutionCallbackWrapper multiExecutionCallback, Member member) {
            this.multiExecutionCallbackWrapper = multiExecutionCallback;
            this.member = member;
        }

        @Override
        public void onResponse(T response) {
            multiExecutionCallbackWrapper.onResponse(member, response);
        }

        @Override
        public void onFailure(Throwable t) {
            multiExecutionCallbackWrapper.onResponse(member, t);
        }
    }

    private static final class MultiExecutionCallbackWrapper implements MultiExecutionCallback {

        private final MultiExecutionCallback multiExecutionCallback;
        private final Map<Member, Object> values;
        private final AtomicInteger members;

        private MultiExecutionCallbackWrapper(int memberSize,
                                              @Nonnull MultiExecutionCallback multiExecutionCallback) {
            checkNotNull(multiExecutionCallback, "multiExecutionCallback must not be null");
            this.multiExecutionCallback = multiExecutionCallback;
            this.values = Collections.synchronizedMap(new HashMap<>(memberSize));
            this.members = new AtomicInteger(memberSize);
        }

        @Override
        public void onResponse(Member member, Object value) {
            multiExecutionCallback.onResponse(member, value);
            values.put(member, value);

            int waitingResponse = members.decrementAndGet();
            if (waitingResponse == 0) {
                onComplete(values);
            }
        }

        @Override
        public void onComplete(Map<Member, Object> values) {
            multiExecutionCallback.onComplete(values);
        }
    }

    private ClientInvocationFuture invokeOnPartitionOwner(ClientMessage request, int partitionId) {
        try {
            ClientInvocation clientInvocation = new ClientInvocation(getClient(), request, getName(), partitionId);
            return clientInvocation.invoke();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private ClientInvocationFuture invokeOnTarget(ClientMessage request, Member target) {
        try {
            ClientInvocation invocation = new ClientInvocation(getClient(), request, getName(), target.getUuid());
            return invocation.invoke();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private UUID getUUID() {
        return UuidUtil.newUnsecureUUID();
    }

    private int getPartitionId(@Nonnull Object key) {
        ClientPartitionService partitionService = getContext().getPartitionService();
        return partitionService.getPartitionId(key);
    }

    private int randomPartitionId() {
        ClientPartitionService partitionService = getContext().getPartitionService();
        return random.nextInt(partitionService.getPartitionCount());
    }
}
