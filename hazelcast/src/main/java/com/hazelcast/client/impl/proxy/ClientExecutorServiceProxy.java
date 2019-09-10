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

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceIsShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToPartitionCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.util.Clock;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.executor.CompletedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * @author ali 5/24/13
 */
public class ClientExecutorServiceProxy extends ClientProxy implements IExecutorService {

    private static final int MIN_TIME_RESOLUTION_OF_CONSECUTIVE_SUBMITS = 10;
    private static final int MAX_CONSECUTIVE_SUBMITS = 100;

    private final Random random = new Random(-System.currentTimeMillis());
    private final AtomicInteger consecutiveSubmits = new AtomicInteger();
    private volatile long lastSubmitTime;

    public ClientExecutorServiceProxy(String serviceName, String objectId, ClientContext context) {
        super(serviceName, objectId, context);
    }

    // execute on members

    @Override
    public void execute(Runnable command) {
        submit(command);
    }

    @Override
    public void executeOnKeyOwner(Runnable command, Object key) {
        submitToKeyOwnerInternal(toData(command), key, null);
    }

    @Override
    public void executeOnMember(Runnable command, Member member) {
        final Address memberAddress = getMemberAddress(member);
        submitToTargetInternal(toData(command), memberAddress, null);
    }

    @Override
    public void executeOnMembers(Runnable command, Collection<Member> members) {
        for (Member member : members) {
            executeOnMember(command, member);
        }
    }

    @Override
    public void execute(Runnable command, MemberSelector memberSelector) {
        List<Member> members = selectMembers(memberSelector);
        int selectedMember = random.nextInt(members.size());
        executeOnMember(command, members.get(selectedMember));
    }

    @Override
    public void executeOnMembers(Runnable command, MemberSelector memberSelector) {
        List<Member> members = selectMembers(memberSelector);
        executeOnMembers(command, members);
    }

    @Override
    public void executeOnAllMembers(Runnable command) {
        final Collection<Member> memberList = getContext().getClusterService().getMemberList();
        for (Member member : memberList) {
            executeOnMember(command, member);
        }
    }


    // submit to members

    @Override
    public <T> Future<T> submitToMember(Callable<T> task, Member member) {
        final Address memberAddress = getMemberAddress(member);
        return submitToTargetInternal(toData(task), memberAddress, null, false);
    }

    @Override
    public <T> Map<Member, Future<T>> submitToMembers(Callable<T> task, Collection<Member> members) {
        Map<Member, Future<T>> futureMap = new HashMap<>(members.size());
        Data taskData = toData(task);
        for (Member member : members) {
            final Address memberAddress = getMemberAddress(member);
            Future<T> f = submitToTargetInternal(taskData, memberAddress, null, true);
            futureMap.put(member, f);
        }
        return futureMap;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task, MemberSelector memberSelector) {
        List<Member> members = selectMembers(memberSelector);
        int selectedMember = random.nextInt(members.size());
        return submitToMember(task, members.get(selectedMember));
    }

    @Override
    public <T> Map<Member, Future<T>> submitToMembers(Callable<T> task, MemberSelector memberSelector) {
        List<Member> members = selectMembers(memberSelector);
        return submitToMembers(task, members);
    }

    @Override
    public <T> Map<Member, Future<T>> submitToAllMembers(Callable<T> task) {
        final Collection<Member> memberList = getContext().getClusterService().getMemberList();
        Map<Member, Future<T>> futureMap = new HashMap<>(memberList.size());
        Data taskData = toData(task);
        for (Member m : memberList) {
            Future<T> f = submitToTargetInternal(taskData, m.getAddress(), null, true);
            futureMap.put(m, f);
        }
        return futureMap;
    }

    // submit to members callback
    @Override
    public void submitToMember(Runnable command, Member member, ExecutionCallback callback) {
        final Address memberAddress = getMemberAddress(member);
        submitToTargetInternal(toData(command), memberAddress, callback);
    }

    @Override
    public void submitToMembers(Runnable command, Collection<Member> members, MultiExecutionCallback callback) {
        MultiExecutionCallbackWrapper multiExecutionCallbackWrapper =
                new MultiExecutionCallbackWrapper(members.size(), callback);
        for (Member member : members) {
            final ExecutionCallbackWrapper executionCallback =
                    new ExecutionCallbackWrapper(multiExecutionCallbackWrapper, member);
            submitToMember(command, member, executionCallback);
        }
    }

    @Override
    public <T> void submitToMember(Callable<T> task, Member member, ExecutionCallback<T> callback) {
        final Address memberAddress = getMemberAddress(member);
        submitToTargetInternal(toData(task), memberAddress, callback);
    }

    @Override
    public <T> void submitToMembers(Callable<T> task, Collection<Member> members, MultiExecutionCallback callback) {
        MultiExecutionCallbackWrapper multiExecutionCallbackWrapper =
                new MultiExecutionCallbackWrapper(members.size(), callback);
        Data taskData = toData(task);
        for (Member member : members) {
            final ExecutionCallbackWrapper<T> executionCallback =
                    new ExecutionCallbackWrapper<T>(multiExecutionCallbackWrapper, member);
            submitToTargetInternal(taskData, getMemberAddress(member), executionCallback);
        }
    }

    @Override
    public void submit(Runnable task, MemberSelector memberSelector, ExecutionCallback callback) {
        List<Member> members = selectMembers(memberSelector);
        int selectedMember = random.nextInt(members.size());
        submitToMember(task, members.get(selectedMember), callback);
    }

    @Override
    public void submitToMembers(Runnable task, MemberSelector memberSelector, MultiExecutionCallback callback) {
        List<Member> members = selectMembers(memberSelector);
        submitToMembers(task, members, callback);
    }

    @Override
    public <T> void submit(Callable<T> task, MemberSelector memberSelector, ExecutionCallback<T> callback) {
        List<Member> members = selectMembers(memberSelector);
        int selectedMember = random.nextInt(members.size());
        submitToMember(task, members.get(selectedMember), callback);
    }

    @Override
    public <T> void submitToMembers(Callable<T> task, MemberSelector memberSelector, MultiExecutionCallback callback) {
        List<Member> members = selectMembers(memberSelector);
        submitToMembers(task, members, callback);
    }

    @Override
    public void submitToAllMembers(Runnable command, MultiExecutionCallback callback) {
        final Collection<Member> memberList = getContext().getClusterService().getMemberList();
        submitToMembers(command, memberList, callback);
    }

    @Override
    public <T> void submitToAllMembers(Callable<T> task, MultiExecutionCallback callback) {
        final Collection<Member> memberList = getContext().getClusterService().getMemberList();
        submitToMembers(task, memberList, callback);
    }

    // submit random

    @Override
    public Future<?> submit(Runnable command) {
        final Object partitionKey = getTaskPartitionKey(command);
        Data taskData = toData(command);
        if (partitionKey != null) {
            return submitToKeyOwnerInternal(taskData, partitionKey, null);
        }
        return submitToRandomInternal(taskData, null, false);
    }

    @Override
    public <T> Future<T> submit(Runnable command, T result) {
        final Object partitionKey = getTaskPartitionKey(command);
        Data taskData = toData(command);
        if (partitionKey != null) {
            return submitToKeyOwnerInternal(taskData, partitionKey, result);
        }
        return submitToRandomInternal(taskData, result, false);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        final Object partitionKey = getTaskPartitionKey(task);
        if (partitionKey != null) {
            return submitToKeyOwner(task, partitionKey);
        }
        return submitToRandomInternal(toData(task), null, false);
    }

    @Override
    public <T> void submit(Runnable command, ExecutionCallback<T> callback) {
        final Object partitionKey = getTaskPartitionKey(command);
        Data task = toData(command);
        if (partitionKey != null) {
            submitToKeyOwnerInternal(task, partitionKey, callback);
        } else {
            submitToRandomInternal(task, callback);
        }
    }

    @Override
    public <T> void submit(Callable<T> task, ExecutionCallback<T> callback) {
        final Object partitionKey = getTaskPartitionKey(task);
        Data taskData = toData(task);
        if (partitionKey != null) {
            submitToKeyOwnerInternal(taskData, partitionKey, callback);
        } else {
            submitToRandomInternal(taskData, callback);
        }
    }

    // submit to key

    @Override
    public <T> Future<T> submitToKeyOwner(Callable<T> task, Object key) {
        return submitToKeyOwnerInternal(toData(task), key, null);
    }

    @Override
    public void submitToKeyOwner(Runnable command, Object key, ExecutionCallback callback) {
        submitToKeyOwnerInternal(toData(command), key, callback);
    }

    @Override
    public <T> void submitToKeyOwner(Callable<T> task, Object key, ExecutionCallback<T> callback) {
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

    @Override
    public List<Runnable> shutdownNow() {
        shutdown();
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        ClientMessage request = ExecutorServiceIsShutdownCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        ExecutorServiceIsShutdownCodec.ResponseParameters resultParameters =
                ExecutorServiceIsShutdownCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean isTerminated() {
        return isShutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        final List<Future<T>> futures = new ArrayList<>(tasks.size());
        final List<Future<T>> result = new ArrayList<>(tasks.size());
        for (Callable<T> task : tasks) {
            futures.add(submitToRandomInternal(toData(task), null, true));
        }
        Executor userExecutor = getContext().getExecutionService().getUserExecutor();
        for (Future<T> future : futures) {
            Object value = retrieveResult(future);
            result.add(new CompletedFuture<T>(getSerializationService(), value, userExecutor));
        }
        return result;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    private Object getTaskPartitionKey(Object task) {
        if (task instanceof PartitionAware) {
            return ((PartitionAware) task).getPartitionKey();
        }
        return null;
    }

    private <T> Future<T> submitToKeyOwnerInternal(Data task, Object key, T defaultValue) {
        checkNotNull(task, "task should not be null");

        String uuid = getUUID();
        int partitionId = getPartitionId(key);
        ClientMessage request = ExecutorServiceSubmitToPartitionCodec.encodeRequest(name, uuid, task);
        ClientInvocationFuture f = invokeOnPartitionOwner(request, partitionId);
        return checkSync(f, uuid, partitionId, false, defaultValue);
    }

    private <T> Future<T> submitToKeyOwnerInternal(Data task, Object key, ExecutionCallback<T> callback) {
        checkNotNull(task, "task should not be null");
        String uuid = getUUID();
        int partitionId = getPartitionId(key);
        ClientMessage request = ExecutorServiceSubmitToPartitionCodec.encodeRequest(name, uuid, task);
        ClientInvocationFuture f = invokeOnPartitionOwner(request, partitionId);

        ClientDelegatingFuture<T> delegatingFuture = new ClientDelegatingFuture<T>(f, getSerializationService(),
                message -> ExecutorServiceSubmitToPartitionCodec.decodeResponse(message).response);
        if (callback != null) {
            delegatingFuture.andThen(callback);
        }
        return delegatingFuture;
    }

    private <T> Future<T> submitToRandomInternal(Data task, T defaultValue, boolean preventSync) {
        checkNotNull(task, "task should not be null");

        String uuid = getUUID();
        int partitionId = randomPartitionId();
        ClientMessage request = ExecutorServiceSubmitToPartitionCodec.encodeRequest(name, uuid, task);
        ClientInvocationFuture f = invokeOnPartitionOwner(request, partitionId);
        return checkSync(f, uuid, partitionId, preventSync, defaultValue);
    }

    private <T> void submitToRandomInternal(Data task, ExecutionCallback<T> callback) {
        checkNotNull(task, "task should not be null");

        String uuid = getUUID();
        int partitionId = randomPartitionId();
        ClientMessage request = ExecutorServiceSubmitToPartitionCodec.encodeRequest(name, uuid, task);
        ClientInvocationFuture f = invokeOnPartitionOwner(request, partitionId);
        ClientDelegatingFuture<T> delegatingFuture = new ClientDelegatingFuture<T>(f, getSerializationService(),
                message -> ExecutorServiceSubmitToPartitionCodec.decodeResponse(message).response);
        delegatingFuture.andThen(callback);
    }

    private <T> Future<T> submitToTargetInternal(Data task, Address address, T defaultValue, boolean preventSync) {
        checkNotNull(task, "task should not be null");

        String uuid = getUUID();
        ClientMessage request = ExecutorServiceSubmitToAddressCodec.encodeRequest(name, uuid, task, address);
        ClientInvocationFuture f = invokeOnTarget(request, address);
        return checkSync(f, uuid, address, preventSync, defaultValue);
    }

    private <T> void submitToTargetInternal(Data task, Address address, ExecutionCallback<T> callback) {
        checkNotNull(task, "task should not be null");

        String uuid = getUUID();
        ClientMessage request = ExecutorServiceSubmitToAddressCodec.encodeRequest(name, uuid, task, address);
        ClientInvocationFuture f = invokeOnTarget(request, address);
        ClientDelegatingFuture<T> delegatingFuture = new ClientDelegatingFuture<T>(f, getSerializationService(),
                message -> ExecutorServiceSubmitToAddressCodec.decodeResponse(message).response);
        if (callback != null) {
            delegatingFuture.andThen(callback);
        }
    }

    @Override
    public String toString() {
        return "IExecutorService{" + "name='" + name + '\'' + '}';
    }

    private <T> Future<T> checkSync(ClientInvocationFuture f, String uuid, Address address,
                                    boolean preventSync, T defaultValue) {
        boolean sync = isSyncComputation(preventSync);
        if (sync) {
            Object response = retrieveResultFromMessage(f);
            Executor userExecutor = getContext().getExecutionService().getUserExecutor();
            return new CompletedFuture<T>(getSerializationService(), response, userExecutor);
        } else {
            return new IExecutorDelegatingFuture<T>(f, getContext(), uuid, defaultValue,
                    message -> ExecutorServiceSubmitToAddressCodec.decodeResponse(message).response, name, address);
        }
    }

    private <T> Future<T> checkSync(ClientInvocationFuture f, String uuid, int partitionId,
                                    boolean preventSync, T defaultValue) {
        boolean sync = isSyncComputation(preventSync);
        if (sync) {
            Object response = retrieveResultFromMessage(f);
            Executor userExecutor = getContext().getExecutionService().getUserExecutor();
            return new CompletedFuture<T>(getSerializationService(), response, userExecutor);
        } else {
            return new IExecutorDelegatingFuture<T>(f, getContext(), uuid, defaultValue,
                    message -> ExecutorServiceSubmitToPartitionCodec.decodeResponse(message).response, name, partitionId);
        }
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

    private Object retrieveResultFromMessage(ClientInvocationFuture f) {
        Object response;
        try {
            SerializationService serializationService = getClient().getSerializationService();
            Data data = ExecutorServiceSubmitToAddressCodec.decodeResponse(f.get()).response;
            response = serializationService.toObject(data);
        } catch (Exception e) {
            response = e;
        }
        return response;
    }

    private boolean isSyncComputation(boolean preventSync) {
        long now = Clock.currentTimeMillis();

        long last = lastSubmitTime;
        lastSubmitTime = now;

        AtomicInteger consecutiveSubmits = this.consecutiveSubmits;

        if (last + MIN_TIME_RESOLUTION_OF_CONSECUTIVE_SUBMITS < now) {
            consecutiveSubmits.set(0);
            return false;
        }

        return !preventSync
                && consecutiveSubmits.incrementAndGet() % MAX_CONSECUTIVE_SUBMITS == 0;
    }

    private List<Member> selectMembers(MemberSelector memberSelector) {
        if (memberSelector == null) {
            throw new IllegalArgumentException("memberSelector must not be null");
        }
        List<Member> selected = new ArrayList<Member>();
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

        private MultiExecutionCallbackWrapper(int memberSize, MultiExecutionCallback multiExecutionCallback) {
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

    private ClientInvocationFuture invokeOnTarget(ClientMessage request, Address target) {
        try {
            ClientInvocation invocation = new ClientInvocation(getClient(), request, getName(), target);
            return invocation.invoke();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private String getUUID() {
        return UuidUtil.newUnsecureUuidString();
    }

    private Address getMemberAddress(Member member) {
        Member m = getContext().getClusterService().getMember(member.getUuid());
        if (m == null) {
            throw new HazelcastException(member + " is not available!");
        }
        return m.getAddress();
    }

    private int getPartitionId(Object key) {
        ClientPartitionService partitionService = getContext().getPartitionService();
        return partitionService.getPartitionId(key);
    }

    private int randomPartitionId() {
        ClientPartitionService partitionService = getContext().getPartitionService();
        return random.nextInt(partitionService.getPartitionCount());
    }
}
