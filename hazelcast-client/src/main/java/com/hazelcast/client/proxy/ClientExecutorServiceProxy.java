/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.util.ClientCancellableDelegatingFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.executor.RunnableAdapter;
import com.hazelcast.executor.client.IsShutdownRequest;
import com.hazelcast.executor.client.PartitionCallableRequest;
import com.hazelcast.executor.client.TargetCallableRequest;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author ali 5/24/13
 */
public class ClientExecutorServiceProxy extends ClientProxy implements IExecutorService {

    private static final int MIN_TIME_RESOLUTION_OF_CONSECUTIVE_SUBMITS = 10;
    private static final int MAX_CONSECUTIVE_SUBMITS = 100;
    private final String name;
    private final Random random = new Random(-System.currentTimeMillis());
    private final AtomicInteger consecutiveSubmits = new AtomicInteger();
    private volatile long lastSubmitTime;

    public ClientExecutorServiceProxy(String instanceName, String serviceName, String objectId) {
        super(instanceName, serviceName, objectId);
        name = objectId;
    }

    // execute on members

    @Override
    public void execute(Runnable command) {
        submit(command);
    }

    @Override
    public void executeOnKeyOwner(Runnable command, Object key) {
        Callable<?> callable = createRunnableAdapter(command);
        submitToKeyOwner(callable, key);
    }

    @Override
    public void executeOnMember(Runnable command, Member member) {
        Callable<?> callable = createRunnableAdapter(command);
        submitToMember(callable, member);
    }

    @Override
    public void executeOnMembers(Runnable command, Collection<Member> members) {
        Callable<?> callable = createRunnableAdapter(command);
        for (Member member : members) {
            submitToMember(callable, member);
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
        Callable<?> callable = createRunnableAdapter(command);
        final Collection<MemberImpl> memberList = getContext().getClusterService().getMemberList();
        for (MemberImpl member : memberList) {
            submitToMember(callable, member);
        }
    }


    // submit to members

    @Override
    public <T> Future<T> submitToMember(Callable<T> task, Member member) {
        final Address memberAddress = getMemberAddress(member);
        return submitToTargetInternal(task, memberAddress, null, false);
    }

    @Override
    public <T> Map<Member, Future<T>> submitToMembers(Callable<T> task, Collection<Member> members) {
        Map<Member, Future<T>> futureMap = new HashMap<Member, Future<T>>(members.size());
        for (Member member : members) {
            final Address memberAddress = getMemberAddress(member);
            Future<T> f = submitToTargetInternal(task, memberAddress, null, true);
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
        final Collection<MemberImpl> memberList = getContext().getClusterService().getMemberList();
        Map<Member, Future<T>> futureMap = new HashMap<Member, Future<T>>(memberList.size());
        for (MemberImpl m : memberList) {
            Future<T> f = submitToTargetInternal(task, m.getAddress(), null, true);
            futureMap.put(m, f);
        }
        return futureMap;
    }

    // submit to members callback
    @Override
    public void submitToMember(Runnable command, Member member, ExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(command);
        submitToMember(callable, member, callback);
    }

    @Override
    public void submitToMembers(Runnable command, Collection<Member> members, MultiExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(command);
        MultiExecutionCallbackWrapper multiExecutionCallbackWrapper =
                new MultiExecutionCallbackWrapper(members.size(), callback);
        for (Member member : members) {
            final ExecutionCallbackWrapper executionCallback =
                    new ExecutionCallbackWrapper(multiExecutionCallbackWrapper, member);
            submitToMember(callable, member, executionCallback);
        }
    }

    @Override
    public <T> void submitToMember(Callable<T> task, Member member, ExecutionCallback<T> callback) {
        final Address memberAddress = getMemberAddress(member);
        submitToTargetInternal(task, memberAddress, callback);
    }

    @Override
    public <T> void submitToMembers(Callable<T> task, Collection<Member> members, MultiExecutionCallback callback) {
        MultiExecutionCallbackWrapper multiExecutionCallbackWrapper =
                new MultiExecutionCallbackWrapper(members.size(), callback);
        for (Member member : members) {
            final ExecutionCallbackWrapper executionCallback =
                    new ExecutionCallbackWrapper(multiExecutionCallbackWrapper, member);
            submitToMember(task, member, executionCallback);
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
        Callable<?> callable = createRunnableAdapter(command);
        submitToAllMembers(callable, callback);
    }

    @Override
    public <T> void submitToAllMembers(Callable<T> task, MultiExecutionCallback callback) {
        final Collection<MemberImpl> memberList = getContext().getClusterService().getMemberList();
        MultiExecutionCallbackWrapper multiExecutionCallbackWrapper =
                new MultiExecutionCallbackWrapper(memberList.size(), callback);
        for (Member member : memberList) {
            final ExecutionCallbackWrapper executionCallback =
                    new ExecutionCallbackWrapper(multiExecutionCallbackWrapper, member);
            submitToMember(task, member, executionCallback);
        }
    }

    // submit random

    public Future<?> submit(Runnable command) {
        final Object partitionKey = getTaskPartitionKey(command);
        Callable<?> callable = createRunnableAdapter(command);
        if (partitionKey != null) {
            return submitToKeyOwner(callable, partitionKey);
        }
        return submitToRandomInternal(callable, null, false);
    }

    public <T> Future<T> submit(Runnable command, T result) {
        final Object partitionKey = getTaskPartitionKey(command);
        Callable<T> callable = createRunnableAdapter(command);
        if (partitionKey != null) {
            return submitToKeyOwnerInternal(callable, partitionKey, result, false);
        }
        return submitToRandomInternal(callable, result, false);
    }

    public <T> Future<T> submit(Callable<T> task) {
        final Object partitionKey = getTaskPartitionKey(task);
        if (partitionKey != null) {
            return submitToKeyOwner(task, partitionKey);
        }
        return submitToRandomInternal(task, null, false);
    }

    public void submit(Runnable command, ExecutionCallback callback) {
        final Object partitionKey = getTaskPartitionKey(command);
        Callable<?> callable = createRunnableAdapter(command);
        if (partitionKey != null) {
            submitToKeyOwnerInternal(callable, partitionKey, callback);
        } else {
            submitToRandomInternal(callable, callback);
        }
    }

    public <T> void submit(Callable<T> task, ExecutionCallback<T> callback) {
        final Object partitionKey = getTaskPartitionKey(task);
        if (partitionKey != null) {
            submitToKeyOwnerInternal(task, partitionKey, callback);
        } else {
            submitToRandomInternal(task, callback);
        }
    }

    // submit to key

    public <T> Future<T> submitToKeyOwner(Callable<T> task, Object key) {
        return submitToKeyOwnerInternal(task, key, null, false);
    }

    public void submitToKeyOwner(Runnable command, Object key, ExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(command);
        submitToKeyOwner(callable, key, callback);
    }

    public <T> void submitToKeyOwner(Callable<T> task, Object key, ExecutionCallback<T> callback) {
        submitToKeyOwnerInternal(task, key, callback);
    }

    // end


    public LocalExecutorStats getLocalExecutorStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    public void shutdown() {
        destroy();
    }

    public List<Runnable> shutdownNow() {
        shutdown();
        return Collections.emptyList();
    }

    public boolean isShutdown() {
        try {
            final IsShutdownRequest request = new IsShutdownRequest(name);
            Boolean result = invoke(request);
            return result;
        } catch (DistributedObjectDestroyedException e) {
            return true;
        }
    }

    public boolean isTerminated() {
        return isShutdown();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        final List<Future<T>> futures = new ArrayList<Future<T>>(tasks.size());
        final List<Future<T>> result = new ArrayList<Future<T>>(tasks.size());
        for (Callable<T> task : tasks) {
            futures.add(submitToRandomInternal(task, null, true));
        }
        ExecutorService asyncExecutor = getContext().getExecutionService().getAsyncExecutor();
        for (Future<T> future : futures) {
            Object value;
            try {
                value = future.get();
            } catch (ExecutionException e) {
                value = e;
            }
            result.add(new CompletedFuture<T>(getContext().getSerializationService(), value, asyncExecutor));
        }
        return result;
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }

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

    private <T> RunnableAdapter<T> createRunnableAdapter(Runnable command) {
        if (command == null) {
            throw new NullPointerException();
        }
        return new RunnableAdapter<T>(command);
    }

    private <T> Future<T> submitToKeyOwnerInternal(Callable<T> task, Object key, T defaultValue, boolean preventSync) {
        checkIfNotNull(task);
        final String uuid = getUUID();
        final int partitionId = getPartitionId(key);
        final PartitionCallableRequest request = new PartitionCallableRequest(name, uuid, task, partitionId);
        final ICompletableFuture<T> f = invokeFuture(request, partitionId);
        return checkSync(f, uuid, null, partitionId, preventSync, defaultValue);
    }

    private <T> void submitToKeyOwnerInternal(Callable<T> task, Object key, ExecutionCallback<T> callback) {
        checkIfNotNull(task);
        final String uuid = getUUID();
        final int partitionId = getPartitionId(key);
        final PartitionCallableRequest request = new PartitionCallableRequest(name, uuid, task, partitionId);
        final ICompletableFuture<T> f = invokeFuture(request, partitionId);
        f.andThen(callback);
    }

    private <T> Future<T> submitToRandomInternal(Callable<T> task, T defaultValue, boolean preventSync) {
        checkIfNotNull(task);
        final String uuid = getUUID();
        final int partitionId = randomPartitionId();
        final PartitionCallableRequest request = new PartitionCallableRequest(name, uuid, task, partitionId);
        final ICompletableFuture<T> f = invokeFuture(request, partitionId);
        return checkSync(f, uuid, null, partitionId, preventSync, defaultValue);
    }

    private <T> void submitToRandomInternal(Callable<T> task, ExecutionCallback<T> callback) {
        checkIfNotNull(task);
        checkIfNotNull(task);
        final String uuid = getUUID();
        final int partitionId = randomPartitionId();
        final PartitionCallableRequest request = new PartitionCallableRequest(name, uuid, task, partitionId);
        final ICompletableFuture<T> f = invokeFuture(request, partitionId);
        f.andThen(callback);
    }

    private <T> Future<T> submitToTargetInternal(Callable<T> task, final Address address, T defaultValue, boolean preventSync) {
        checkIfNotNull(task);
        final String uuid = getUUID();
        final TargetCallableRequest request = new TargetCallableRequest(name, uuid, task, address);
        ICompletableFuture<T> f = invokeFuture(request);
        return checkSync(f, uuid, address, -1, preventSync, defaultValue);
    }

    private <T> void submitToTargetInternal(Callable<T> task, final Address address, final ExecutionCallback<T> callback) {
        checkIfNotNull(task);
        final TargetCallableRequest request = new TargetCallableRequest(name, null, task, address);
        ICompletableFuture<T> f = invokeFuture(request);
        f.andThen(callback);
    }

    private void checkIfNotNull(Callable task) {
        if (task == null) {
            throw new NullPointerException();
        }
    }

    @Override
    public String toString() {
        return "IExecutorService{" + "name='" + getName() + '\'' + '}';
    }

    private <T> Future<T> checkSync(ICompletableFuture<T> f, String uuid,
                                    Address address, int partitionId, boolean preventSync, T defaultValue) {
        boolean sync = false;
        final long last = lastSubmitTime;
        final long now = Clock.currentTimeMillis();
        if (last + MIN_TIME_RESOLUTION_OF_CONSECUTIVE_SUBMITS < now) {
            consecutiveSubmits.set(0);
        } else if (consecutiveSubmits.incrementAndGet() % MAX_CONSECUTIVE_SUBMITS == 0) {
            sync = true;
        }
        lastSubmitTime = now;

        if (sync && !preventSync) {
            Object response;
            try {
                response = f.get();
            } catch (Exception e) {
                response = e;
            }
            ExecutorService asyncExecutor = getContext().getExecutionService().getAsyncExecutor();
            return new CompletedFuture<T>(getContext().getSerializationService(), response, asyncExecutor);
        }
        if (defaultValue != null) {
            return new ClientCancellableDelegatingFuture<T>(f, getContext(), uuid, address, partitionId, defaultValue);
        }
        return new ClientCancellableDelegatingFuture<T>(f, getContext(), uuid, address, partitionId);
    }

    private List<Member> selectMembers(MemberSelector memberSelector) {
        if (memberSelector == null) {
            throw new IllegalArgumentException("memberSelector must not be null");
        }
        List<Member> selected = new ArrayList<Member>();
        Collection<MemberImpl> members = getContext().getClusterService().getMemberList();
        for (MemberImpl member : members) {
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

        public void onResponse(T response) {
            multiExecutionCallbackWrapper.onResponse(member, response);
        }

        public void onFailure(Throwable t) {
        }
    }

    private static final class MultiExecutionCallbackWrapper implements MultiExecutionCallback {

        private final AtomicInteger members;
        private final MultiExecutionCallback multiExecutionCallback;
        private final Map<Member, Object> values;

        private MultiExecutionCallbackWrapper(int memberSize, MultiExecutionCallback multiExecutionCallback) {
            this.multiExecutionCallback = multiExecutionCallback;
            this.members = new AtomicInteger(memberSize);
            values = new HashMap<Member, Object>(memberSize);
        }

        public void onResponse(Member member, Object value) {
            multiExecutionCallback.onResponse(member, value);
            values.put(member, value);
            int waitingResponse = members.decrementAndGet();
            if (waitingResponse == 0) {
                onComplete(values);
            }
        }

        public void onComplete(Map<Member, Object> values) {
            multiExecutionCallback.onComplete(values);
        }
    }

    private ICompletableFuture invokeFuture(PartitionCallableRequest request, int partitionId) {
        try {
            final Address partitionOwner = getPartitionOwner(partitionId);
            return getContext().getInvocationService().invokeOnTarget(request, partitionOwner);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private ICompletableFuture invokeFuture(TargetCallableRequest request) {
        try {
            return getContext().getInvocationService().invokeOnTarget(request, request.getTarget());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private String getUUID() {
        return UuidUtil.buildRandomUuidString();
    }

    private Address getMemberAddress(Member member) {
        MemberImpl m = getContext().getClusterService().getMember(member.getUuid());
        if (m == null) {
            throw new HazelcastException(member + " is not available!!!");
        }
        return m.getAddress();
    }

    private int getPartitionId(Object key) {
        final ClientPartitionService partitionService = getContext().getPartitionService();
        return partitionService.getPartitionId(key);
    }

    private int randomPartitionId() {
        final ClientPartitionService partitionService = getContext().getPartitionService();
        return random.nextInt(partitionService.getPartitionCount());
    }

    private Address getPartitionOwner(int partitionId) {
        final ClientPartitionService partitionService = getContext().getPartitionService();
        return partitionService.getPartitionOwner(partitionId);
    }

}
