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

package com.hazelcast.executor;

import com.hazelcast.core.*;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.executor.CompletedFuture;
import com.hazelcast.util.executor.DelegatingFuture;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @mdogan 1/17/13
 */
public class ExecutorServiceProxy extends AbstractDistributedObject<DistributedExecutorService> implements IExecutorService {

    private final String name;
    private final Random random = new Random();
    private final int partitionCount;
    private final AtomicInteger consecutiveSubmits = new AtomicInteger();
    private volatile long lastSubmitTime = 0L;

    public ExecutorServiceProxy(String name, NodeEngine nodeEngine, DistributedExecutorService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
    }

    public void execute(Runnable command) {
        Callable<?> callable = createRunnableAdapter(command);
        submit(callable);
    }

    private <T> RunnableAdapter<T> createRunnableAdapter(Runnable command) {
        if (command == null) throw new NullPointerException();
        return new RunnableAdapter<T>(command);
    }

    public void executeOnKeyOwner(Runnable command, Object key) {
        Callable<?> callable = createRunnableAdapter(command);
        submitToKeyOwner(callable, key);
    }

    public void executeOnMember(Runnable command, Member member) {
        Callable<?> callable = createRunnableAdapter(command);
        submitToMember(callable, member);
    }

    public void executeOnMembers(Runnable command, Collection<Member> members) {
        Callable<?> callable = createRunnableAdapter(command);
        submitToMembers(callable, members);
    }

    public void executeOnAllMembers(Runnable command) {
        Callable<?> callable = createRunnableAdapter(command);
        submitToAllMembers(callable);
    }

    public Future<?> submit(Runnable task) {
        Callable<?> callable = createRunnableAdapter(task);
        return submit(callable);
    }

    public <T> Future<T> submit(Runnable task, T result) {
        Callable<T> callable = createRunnableAdapter(task);
        return new DelegatingFuture<T>(submit(callable), getNodeEngine().getSerializationService(), result);
    }

    public <T> Future<T> submit(Callable<T> task) {
        final int partitionId = getTaskPartitionId(task);
        return submitToPartitionOwner(task, partitionId);
    }

    private <T> Future<T> submitToPartitionOwner(Callable<T> task, int partitionId) {
        if (task == null) throw new NullPointerException();
        if (isShutdown()) {
            throw new RejectedExecutionException(getRejectionMessage());
        }
        final NodeEngine nodeEngine = getNodeEngine();
        Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(DistributedExecutorService.SERVICE_NAME,
                new CallableTaskOperation<T>(name, task), partitionId).build();
        return invoke(inv);
    }

    private <T> Future<T> invoke(Invocation inv) {
        final NodeEngine nodeEngine = getNodeEngine();
        final boolean sync = checkSync();
        final Future future = inv.invoke();
        if (sync) {
            Object response;
            try {
                response = future.get();
            } catch (Exception e) {
                response = e;
            }
            return new CompletedFuture<T>(nodeEngine.getSerializationService(), response);
        }
        return new DelegatingFuture<T>(future, nodeEngine.getSerializationService());
    }

    private boolean checkSync() {
        boolean sync = false;
        final long last = lastSubmitTime;
        final long now = Clock.currentTimeMillis();
        if (last + 10 < now) {
            consecutiveSubmits.set(0);
        } else if (consecutiveSubmits.incrementAndGet() % 100 == 0) {
            sync = true;
        }
        lastSubmitTime = now;
        return sync;
    }

    private <T> int getTaskPartitionId(Callable<T> task) {
        final int partitionId;
        if (task instanceof PartitionAware) {
            final Object partitionKey = ((PartitionAware) task).getPartitionKey();
            partitionId = getNodeEngine().getPartitionService().getPartitionId(partitionKey);
        } else {
            partitionId = random.nextInt(partitionCount);
        }
        return partitionId;
    }

    public <T> Future<T> submitToKeyOwner(Callable<T> task, Object key) {
        final NodeEngine nodeEngine = getNodeEngine();
        return submitToPartitionOwner(task, nodeEngine.getPartitionService().getPartitionId(key));
    }

    public <T> Future<T> submitToMember(Callable<T> task, Member member) {
        if (task == null) throw new NullPointerException();
        if (isShutdown()) {
            throw new RejectedExecutionException(getRejectionMessage());
        }
        final NodeEngine nodeEngine = getNodeEngine();
        Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(DistributedExecutorService.SERVICE_NAME,
                new MemberCallableTaskOperation<T>(name, task), ((MemberImpl) member).getAddress()).build();
        return invoke(inv);
    }

    public <T> Map<Member, Future<T>> submitToMembers(Callable<T> task, Collection<Member> members) {
        final Map<Member, Future<T>> futures = new HashMap<Member, Future<T>>(members.size());
        for (Member member : members) {
            futures.put(member, submitToMember(task, member));
        }
        return futures;
    }

    public <T> Map<Member, Future<T>> submitToAllMembers(Callable<T> task) {
        final NodeEngine nodeEngine = getNodeEngine();
        return submitToMembers(task, nodeEngine.getClusterService().getMembers());
    }

    public void submit(Runnable task, ExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(task);
        submit(callable, callback);
    }

    public void submitToKeyOwner(Runnable task, Object key, ExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(task);
        submitToKeyOwner(callable, key, callback);
    }

    public void submitToMember(Runnable task, Member member, ExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(task);
        submitToMember(callable, member, callback);
    }

    public void submitToMembers(Runnable task, Collection<Member> members, MultiExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(task);
        submitToMembers(callable, members, callback);
    }

    public void submitToAllMembers(Runnable task, MultiExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(task);
        submitToAllMembers(callable, callback);
    }

    private <T> void submitToPartitionOwner(Callable<T> task, ExecutionCallback<T> callback, int partitionId) {
        if (isShutdown()) {
            throw new RejectedExecutionException(getRejectionMessage());
        }
        final NodeEngine nodeEngine = getNodeEngine();
        Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(DistributedExecutorService.SERVICE_NAME,
                new CallableTaskOperation<T>(name, task), partitionId).setCallback(new ExecutionCallbackAdapter(callback)).build();
        inv.invoke();
    }

    public <T> void submit(Callable<T> task, ExecutionCallback<T> callback) {
        final int partitionId = getTaskPartitionId(task);
        submitToPartitionOwner(task, callback, partitionId);
    }

    public <T> void submitToKeyOwner(Callable<T> task, Object key, ExecutionCallback<T> callback) {
        final NodeEngine nodeEngine = getNodeEngine();
        submitToPartitionOwner(task, callback, nodeEngine.getPartitionService().getPartitionId(key));
    }

    public <T> void submitToMember(Callable<T> task, Member member, ExecutionCallback<T> callback) {
        if (isShutdown()) {
            throw new RejectedExecutionException(getRejectionMessage());
        }
        final NodeEngine nodeEngine = getNodeEngine();
        Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(DistributedExecutorService.SERVICE_NAME,
                new MemberCallableTaskOperation<T>(name, task), ((MemberImpl) member).getAddress())
                .setCallback(new ExecutionCallbackAdapter(callback)).build();
        inv.invoke();
    }

    private String getRejectionMessage() {
        return "ExecutorService[" + name + "] is shutdown! In order to create a new ExecutorService with name '" +
                name + "', you need to destroy current ExecutorService first!";
    }

    public <T> void submitToMembers(Callable<T> task, Collection<Member> members, MultiExecutionCallback callback) {
        final NodeEngine nodeEngine = getNodeEngine();
        ExecutionCallbackAdapterFactory executionCallbackFactory = new ExecutionCallbackAdapterFactory(nodeEngine,
                members, callback);
        for (Member member : members) {
            submitToMember(task, member, executionCallbackFactory.<T>callbackFor(member));
        }
    }

    public <T> void submitToAllMembers(Callable<T> task, MultiExecutionCallback callback) {
        final NodeEngine nodeEngine = getNodeEngine();
        submitToMembers(task, nodeEngine.getClusterService().getMembers(), callback);
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        final List<Future<T>> futures = new ArrayList<Future<T>>(tasks.size());
        final List<Future<T>> result = new ArrayList<Future<T>>(tasks.size());
        for (Callable<T> task : tasks) {
            futures.add(submit(task));
        }
        for (Future<T> future : futures) {
            Object value;
            try {
                value = future.get();
            } catch (ExecutionException e) {
                value = e;
            }
            result.add(new CompletedFuture<T>(getNodeEngine().getSerializationService(), value));
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

    protected RuntimeException throwNotActiveException() {
        throw new RejectedExecutionException();
    }

    public boolean isShutdown() {
        try {
            return getService().isShutdown(name);
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

    public void shutdown() {
        final NodeEngine nodeEngine = getNodeEngine();
        final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        final OperationService operationService = nodeEngine.getOperationService();
        final Collection<Future> calls = new LinkedList<Future>();
        for (MemberImpl member : members) {
            if (member.localMember()) {
                getService().shutdownExecutor(name);
            } else {
                Future f = operationService.createInvocationBuilder(getServiceName(), new ShutdownOperation(name),
                        member.getAddress()).build().invoke();
                calls.add(f);
            }
        }
        for (Future f : calls) {
            try {
                f.get(1, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
        }
    }

    public List<Runnable> shutdownNow() {
        shutdown();
        return Collections.emptyList();
    }

    public LocalExecutorStats getLocalExecutorStats() {
        return getService().getLocalExecutorStats(name);
    }

    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    public Object getId() {
        return name;
    }

    public String getName() {
        return name;
    }
}
