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
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;

import java.util.*;
import java.util.concurrent.*;

/**
 * @mdogan 1/17/13
 */
public class ExecutorServiceProxy extends AbstractDistributedObject<DistributedExecutorService> implements IExecutorService {

    private final String name;
    private final Random random = new Random();
    private final int partitionCount;

    public ExecutorServiceProxy(String name, NodeEngine nodeEngine) {
        super(nodeEngine, null);
        this.name = name;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
    }

    public void execute(Runnable command) {
        Callable<?> callable = new RunnableAdapter(command);
        submit(callable);
    }

    public void executeOnKeyOwner(Runnable command, Object key) {
        Callable<?> callable = new RunnableAdapter(command);
        submitToKeyOwner(callable, key);
    }

    public void executeOnMember(Runnable command, Member member) {
        Callable<?> callable = new RunnableAdapter(command);
        submitToMember(callable, member);
    }

    public void executeOnMembers(Runnable command, Collection<Member> members) {
        Callable<?> callable = new RunnableAdapter(command);
        submitToMembers(callable, members);
    }

    public void executeOnAllMembers(Runnable command) {
        Callable<?> callable = new RunnableAdapter(command);
        submitToAllMembers(callable);
    }

    public Future<?> submit(Runnable task) {
        Callable<?> callable = new RunnableAdapter(task);
        return submit(callable);
    }

    public <T> Future<T> submit(Runnable task, T result) {
        // TODO: Future should return given result!
        Callable<T> callable = new RunnableAdapter<T>(task, result);
        return submit(callable);
    }

    private <T> Future<T> submitToPartitionOwner(Callable<T> task, int partitionId) {
        final NodeEngine nodeEngine = getNodeEngine();
        Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(DistributedExecutorService.SERVICE_NAME,
                new CallableTaskOperation<T>(name, task), partitionId).build();
        return new FutureProxy<T>(inv.invoke(), nodeEngine);
    }

    public <T> Future<T> submit(Callable<T> task) {
        final int partitionId = getTaskPartitionId(task);
        return submitToPartitionOwner(task, partitionId);
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
        final NodeEngine nodeEngine = getNodeEngine();
        Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(DistributedExecutorService.SERVICE_NAME,
                new MemberCallableTaskOperation<T>(name, task), ((MemberImpl) member).getAddress()).build();
        return new FutureProxy<T>(inv.invoke(), nodeEngine);
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
        Callable<?> callable = new RunnableAdapter(task);
        submit(callable, callback);
    }

    public void submitToKeyOwner(Runnable task, Object key, ExecutionCallback callback) {
        Callable<?> callable = new RunnableAdapter(task);
        submitToKeyOwner(callable, key, callback);
    }

    public void submitToMember(Runnable task, Member member, ExecutionCallback callback) {
        Callable<?> callable = new RunnableAdapter(task);
        submitToMember(callable, member, callback);
    }

    public void submitToMembers(Runnable task, Collection<Member> members, MultiExecutionCallback callback) {
        Callable<?> callable = new RunnableAdapter(task);
        submitToMembers(callable, members, callback);
    }

    public void submitToAllMembers(Runnable task, MultiExecutionCallback callback) {
        Callable<?> callable = new RunnableAdapter(task);
        submitToAllMembers(callable, callback);
    }

    private <T> void submitToPartitionOwner(Callable<T> task, ExecutionCallback<T> callback, int partitionId) {
        final NodeEngine nodeEngine = getNodeEngine();
        Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(DistributedExecutorService.SERVICE_NAME,
                new CallableTaskOperation<T>(name, task), partitionId).build();
        nodeEngine.getAsyncInvocationService().invoke(inv, callback);
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
        final NodeEngine nodeEngine = getNodeEngine();
        Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(DistributedExecutorService.SERVICE_NAME,
                new MemberCallableTaskOperation<T>(name, task), ((MemberImpl) member).getAddress()).build();
        nodeEngine.getAsyncInvocationService().invoke(inv, callback);
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
        throw new UnsupportedOperationException();
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

    public boolean isShutdown() {
        return false;
    }

    public boolean isTerminated() {
        return false;
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    public void shutdown() {
        destroy();
    }

    public List<Runnable> shutdownNow() {
        destroy();
        return null;
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
