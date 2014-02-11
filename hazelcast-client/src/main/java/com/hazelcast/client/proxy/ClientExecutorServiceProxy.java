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

import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.util.ClientCancellableDelegatingFuture;
import com.hazelcast.core.*;
import com.hazelcast.executor.RunnableAdapter;
import com.hazelcast.executor.client.IsShutdownRequest;
import com.hazelcast.executor.client.TargetCallableRequest;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.nio.Address;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.executor.CompletedFuture;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author ali 5/24/13
 */
public class ClientExecutorServiceProxy extends ClientProxy implements IExecutorService {

    private final String name;
    private final Random random = new Random(-System.currentTimeMillis());
    private final AtomicInteger consecutiveSubmits = new AtomicInteger();
    private volatile long lastSubmitTime = 0L;


    public ClientExecutorServiceProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
        name = objectId;
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

    public Future<?> submit(Runnable command) {
        final Address partitionOwner = getTaskPartitionOwner(command);
        Callable<?> callable = createRunnableAdapter(command);
        return submitToTargetInternal(callable, partitionOwner, false);
    }

    public <T> Future<T> submit(Runnable command, T result) {
        final Address partitionOwner = getTaskPartitionOwner(command);
        Callable<T> callable = createRunnableAdapter(command);
        return submitToTargetInternal(callable, partitionOwner, false, result);
    }

    public void execute(Runnable command) {
        final Address partitionOwner = getTaskPartitionOwner(command);
        Callable<?> callable = createRunnableAdapter(command);
        submitToTargetInternal(callable, partitionOwner, false);
    }

    public void executeOnKeyOwner(Runnable command, Object key) {
        Callable<?> callable = createRunnableAdapter(command);
        submitToTargetInternal(callable, getPartitionOwner(key), false);
    }

    public void executeOnMember(Runnable command, Member member) {
        Callable<?> callable = createRunnableAdapter(command);
        MemberImpl m = getContext().getClusterService().getMember(member.getUuid());
        if (m != null) {
            submitToTargetInternal(callable, m.getAddress(), false);
        } else {
            throw new HazelcastException("Member is not available!!!");
        }
    }

    public void executeOnMembers(Runnable command, Collection<Member> members) {
        for (Member member : members) {
            executeOnMember(command, member);
        }
    }

    public void executeOnAllMembers(Runnable command) {
        Callable<?> callable = createRunnableAdapter(command);
        final Collection<MemberImpl> memberList = getContext().getClusterService().getMemberList();
        for (MemberImpl m : memberList) {
            submitToTargetInternal(callable, m.getAddress(), false);
        }
    }

    public <T> Future<T> submit(Callable<T> task) {
        final Address partitionOwner = getTaskPartitionOwner(task);
        return submitToTargetInternal(task, partitionOwner, false);
    }

    public <T> Future<T> submitToKeyOwner(Callable<T> task, Object key) {
        return submitToTargetInternal(task, getPartitionOwner(key), false);
    }

    public <T> Future<T> submitToMember(Callable<T> task, Member member) {
        MemberImpl m = getContext().getClusterService().getMember(member.getUuid());
        if (m != null) {
            return submitToTargetInternal(task, m.getAddress(), false);
        } else {
            throw new HazelcastException("Member is not available!!!");
        }
    }

    public <T> Map<Member, Future<T>> submitToMembers(Callable<T> task, Collection<Member> members) {
        Map<Member, Future<T>> futureMap = new HashMap<Member, Future<T>>(members.size());
        final ClientClusterService clusterService = getContext().getClusterService();
        for (Member m : members) {
            final MemberImpl member = clusterService.getMember(m.getUuid());
            if (member == null) {
                throw new HazelcastException("Member is not available!!!");
            }
            Future<T> f = submitToTargetInternal(task, member.getAddress(), true);
            futureMap.put(m, f);
        }
        return futureMap;
    }

    public <T> Map<Member, Future<T>> submitToAllMembers(Callable<T> task) {
        final Collection<MemberImpl> memberList = getContext().getClusterService().getMemberList();
        Map<Member, Future<T>> futureMap = new HashMap<Member, Future<T>>(memberList.size());
        for (MemberImpl m : memberList) {
            Future<T> f = submitToTargetInternal(task, m.getAddress(), true);
            futureMap.put(m, f);
        }
        return futureMap;
    }

    public void submit(Runnable command, ExecutionCallback callback) {
        final Address partitionOwner = getTaskPartitionOwner(command);
        Callable<?> callable = createRunnableAdapter(command);
        submitToTargetInternal(callable, partitionOwner, callback);
    }

    public void submitToKeyOwner(Runnable command, Object key, ExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(command);
        submitToTargetInternal(callable, getPartitionOwner(key), callback);
    }

    public void submitToMember(Runnable command, Member member, ExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(command);
        MemberImpl m = getContext().getClusterService().getMember(member.getUuid());
        if (m != null) {
            submitToTargetInternal(callable, m.getAddress(), callback);
        } else {
            throw new HazelcastException("Member is not available!!!");
        }
    }

    public void submitToMembers(Runnable command, Collection<Member> members, MultiExecutionCallback callback) {
        MultiExecutionCallbackWrapper multiExecutionCallbackWrapper = new MultiExecutionCallbackWrapper(members.size(), callback);
        for (Member member : members) {
            final ExecutionCallbackWrapper executionCallback = new ExecutionCallbackWrapper(multiExecutionCallbackWrapper, member);
            submitToMember(command, member, executionCallback);
        }
    }

    public void submitToAllMembers(Runnable command, MultiExecutionCallback callback) {
        final Collection<MemberImpl> memberList = getContext().getClusterService().getMemberList();
        MultiExecutionCallbackWrapper multiExecutionCallbackWrapper = new MultiExecutionCallbackWrapper(memberList.size(), callback);
        for (Member member : memberList) {
            final ExecutionCallbackWrapper executionCallback = new ExecutionCallbackWrapper(multiExecutionCallbackWrapper, member);
            submitToMember(command, member, executionCallback);
        }
    }

    public <T> void submit(Callable<T> task, ExecutionCallback<T> callback) {
        final Address partitionOwner = getTaskPartitionOwner(task);
        submitToTargetInternal(task, partitionOwner, callback);
    }

    public <T> void submitToKeyOwner(Callable<T> task, Object key, ExecutionCallback<T> callback) {
        submitToTargetInternal(task, getPartitionOwner(key), callback);
    }

    public <T> void submitToMember(Callable<T> task, Member member, ExecutionCallback<T> callback) {
        MemberImpl m = getContext().getClusterService().getMember(member.getUuid());
        if (m != null) {
            submitToTargetInternal(task, m.getAddress(), callback);
        } else {
            throw new HazelcastException("Member is not available!!!");
        }
    }

    public <T> void submitToMembers(Callable<T> task, Collection<Member> members, MultiExecutionCallback callback) {
        MultiExecutionCallbackWrapper multiExecutionCallbackWrapper = new MultiExecutionCallbackWrapper(members.size(), callback);
        for (Member member : members) {
            final ExecutionCallbackWrapper executionCallback = new ExecutionCallbackWrapper(multiExecutionCallbackWrapper, member);
            submitToMember(task, member, executionCallback);
        }
    }

    public <T> void submitToAllMembers(Callable<T> task, MultiExecutionCallback callback) {
        final Collection<MemberImpl> memberList = getContext().getClusterService().getMemberList();
        MultiExecutionCallbackWrapper multiExecutionCallbackWrapper = new MultiExecutionCallbackWrapper(memberList.size(), callback);
        for (Member member : memberList) {
            final ExecutionCallbackWrapper executionCallback = new ExecutionCallbackWrapper(multiExecutionCallbackWrapper, member);
            submitToMember(task, member, executionCallback);
        }
    }

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
        } catch (HazelcastInstanceNotActiveException e) {
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
            final Address partitionOwner = getTaskPartitionOwner(task);
            futures.add(submitToTargetInternal(task, partitionOwner, true));
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

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    protected void onDestroy() {
    }

    private Address getTaskPartitionOwner(Object task) {
        if (task instanceof PartitionAware) {
            final Object partitionKey = ((PartitionAware) task).getPartitionKey();
            return getPartitionOwner(partitionKey);
        }
        final ClientPartitionService partitionService = getContext().getPartitionService();
        final int partitionId = random.nextInt(partitionService.getPartitionCount());
        return partitionService.getPartitionOwner(partitionId);
    }

    private Address getPartitionOwner(Object key) {
        final ClientPartitionService partitionService = getContext().getPartitionService();
        final int partitionId = partitionService.getPartitionId(key);
        return partitionService.getPartitionOwner(partitionId);
    }

    private <T> RunnableAdapter<T> createRunnableAdapter(Runnable command) {
        if (command == null) throw new NullPointerException();
        return new RunnableAdapter<T>(command);
    }

    private <T> ICompletableFuture<T> submitToTargetInternal(Callable<T> task, final Address address,
                                                             boolean preventSync) {
        return submitToTargetInternal(task, address, preventSync, null);
    }

    private <T> ICompletableFuture<T> submitToTargetInternal(Callable<T> task, final Address address,
                                                             boolean preventSync, T defaultValue) {
        check(task);
        final String uuid = getUUID();
        final TargetCallableRequest request = new TargetCallableRequest(name, uuid, task, address);
        ICompletableFuture<T> f = invokeFuture(request);
        return checkSync(f, uuid, address, preventSync, defaultValue);
    }

    private <T> void submitToTargetInternal(Callable<T> task, final Address address, final ExecutionCallback<T> callback) {
        check(task);
        final TargetCallableRequest request = new TargetCallableRequest(name, null, task, address);
        ICompletableFuture f = invokeFuture(request);
        f.andThen(callback);
    }

    private void check(Callable task) {
        if (task == null) {
            throw new NullPointerException();
        }
    }

    @Override
    public String toString() {
        return "IExecutorService{" + "name='" + getName() + '\'' + '}';
    }

    private <T> ICompletableFuture<T> checkSync(ICompletableFuture<T> f, String uuid,
                                                Address address, boolean preventSync, T defaultValue) {
        boolean sync = false;
        final long last = lastSubmitTime;
        final long now = Clock.currentTimeMillis();
        if (last + 10 < now) {
            consecutiveSubmits.set(0);
        } else if (consecutiveSubmits.incrementAndGet() % 100 == 0) {
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
            return new ClientCancellableDelegatingFuture<T>(f, getContext(), uuid, address, defaultValue);
        } else {
            return new ClientCancellableDelegatingFuture<T>(f, getContext(), uuid, address);
        }
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
        return selected;
    }

    private static class ExecutionCallbackWrapper<T> implements ExecutionCallback<T> {
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

    private static class MultiExecutionCallbackWrapper implements MultiExecutionCallback {

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

}
