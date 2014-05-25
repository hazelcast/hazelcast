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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.executor.CompletedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.hazelcast.util.UuidUtil.buildRandomUuidString;

public class ExecutorServiceProxy
        extends AbstractDistributedObject<DistributedExecutorService>
        implements IExecutorService {

    private static final AtomicIntegerFieldUpdater<ExecutorServiceProxy> CONSECUTIVE_SUBMITS_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(ExecutorServiceProxy.class, "consecutiveSubmits");

    public static final int SYNC_FREQUENCY = 100;
    private final String name;
    private final Random random = new Random(-System.currentTimeMillis());
    private final int partitionCount;
    private final ILogger logger;

    // This field is never accessed directly but by the UPDATER above
    private volatile int consecutiveSubmits = 0;

    private volatile long lastSubmitTime;

    public ExecutorServiceProxy(String name, NodeEngine nodeEngine, DistributedExecutorService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.logger = nodeEngine.getLogger(ExecutorServiceProxy.class);
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

    @Override
    public void execute(Runnable command) {
        Callable<?> callable = createRunnableAdapter(command);
        submit(callable);
    }

    private <T> RunnableAdapter<T> createRunnableAdapter(Runnable command) {
        if (command == null) {
            throw new NullPointerException();
        }
        return new RunnableAdapter<T>(command);
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
        submitToMembers(callable, members);
    }

    @Override
    public void executeOnAllMembers(Runnable command) {
        Callable<?> callable = createRunnableAdapter(command);
        submitToAllMembers(callable);
    }

    @Override
    public Future<?> submit(Runnable task) {
        Callable<?> callable = createRunnableAdapter(task);
        return submit(callable);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        if (task == null) {
            throw new NullPointerException();
        }
        if (isShutdown()) {
            throw new RejectedExecutionException(getRejectionMessage());
        }

        Callable<T> callable = createRunnableAdapter(task);
        NodeEngine nodeEngine = getNodeEngine();
        String uuid = buildRandomUuidString();
        int partitionId = getTaskPartitionId(callable);

        CallableTaskOperation op = new CallableTaskOperation(name, uuid, callable);
        ICompletableFuture future = invoke(partitionId, op);
        boolean sync = checkSync();
        if (sync) {
            try {
                future.get();
            } catch (Exception exception) {
                logger.warning(exception);
            }
            return new CompletedFuture<T>(nodeEngine.getSerializationService(), result, getAsyncExecutor());
        }
        return new CancellableDelegatingFuture<T>(future, result, nodeEngine, uuid, partitionId);
    }

    private InternalCompletableFuture invoke(int partitionId, CallableTaskOperation op) {
        NodeEngine nodeEngine = getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();
        return operationService.invokeOnPartition(DistributedExecutorService.SERVICE_NAME, op, partitionId);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        final int partitionId = getTaskPartitionId(task);
        return submitToPartitionOwner(task, partitionId, false);
    }

    private <T> Future<T> submitToPartitionOwner(Callable<T> task, int partitionId, boolean preventSync) {
        if (task == null) {
            throw new NullPointerException();
        }
        if (isShutdown()) {
            throw new RejectedExecutionException(getRejectionMessage());
        }
        NodeEngine nodeEngine = getNodeEngine();
        String uuid = buildRandomUuidString();

        boolean sync = !preventSync && checkSync();
        CallableTaskOperation op = new CallableTaskOperation(name, uuid, task);
        ICompletableFuture future = invoke(partitionId, op);
        if (sync) {
            Object response;
            try {
                response = future.get();
            } catch (Exception e) {
                response = e;
            }
            return new CompletedFuture<T>(nodeEngine.getSerializationService(), response, getAsyncExecutor());
        }
        return new CancellableDelegatingFuture<T>(future, nodeEngine, uuid, partitionId);
    }

    /**
     * This is a hack to prevent overloading the system with unprocessed tasks. Once backpressure is added, this can
     * be removed.
     */
    private boolean checkSync() {
        boolean sync = false;
        long last = lastSubmitTime;
        long now = Clock.currentTimeMillis();
        if (last + 10 < now) {
            CONSECUTIVE_SUBMITS_UPDATER.set(this, 0);
        } else if (CONSECUTIVE_SUBMITS_UPDATER.incrementAndGet(this) % SYNC_FREQUENCY == 0) {
            sync = true;
        }
        lastSubmitTime = now;
        return sync;
    }

    private <T> int getTaskPartitionId(Callable<T> task) {
        int partitionId;
        if (task instanceof PartitionAware) {
            final Object partitionKey = ((PartitionAware) task).getPartitionKey();
            partitionId = getNodeEngine().getPartitionService().getPartitionId(partitionKey);
        } else {
            partitionId = random.nextInt(partitionCount);
        }
        return partitionId;
    }

    @Override
    public <T> Future<T> submitToKeyOwner(Callable<T> task, Object key) {
        NodeEngine nodeEngine = getNodeEngine();
        return submitToPartitionOwner(task, nodeEngine.getPartitionService().getPartitionId(key), false);
    }

    @Override
    public <T> Future<T> submitToMember(Callable<T> task, Member member) {
        if (task == null) {
            throw new NullPointerException();
        }
        if (isShutdown()) {
            throw new RejectedExecutionException(getRejectionMessage());
        }
        NodeEngine nodeEngine = getNodeEngine();
        String uuid = buildRandomUuidString();
        Address target = ((MemberImpl) member).getAddress();

        boolean sync = checkSync();
        MemberCallableTaskOperation op = new MemberCallableTaskOperation(name, uuid, task);
        InternalCompletableFuture future = nodeEngine.getOperationService()
                                                     .invokeOnTarget(DistributedExecutorService.SERVICE_NAME, op, target);
        if (sync) {
            Object response;
            try {
                response = future.get();
            } catch (Exception e) {
                response = e;
            }
            return new CompletedFuture<T>(nodeEngine.getSerializationService(), response, getAsyncExecutor());
        }
        return new CancellableDelegatingFuture<T>(future, nodeEngine, uuid, target);
    }

    @Override
    public <T> Map<Member, Future<T>> submitToMembers(Callable<T> task, Collection<Member> members) {
        Map<Member, Future<T>> futures = new HashMap<Member, Future<T>>(members.size());
        for (Member member : members) {
            futures.put(member, submitToMember(task, member));
        }
        return futures;
    }

    @Override
    public <T> Map<Member, Future<T>> submitToAllMembers(Callable<T> task) {
        NodeEngine nodeEngine = getNodeEngine();
        return submitToMembers(task, nodeEngine.getClusterService().getMembers());
    }

    @Override
    public void submit(Runnable task, ExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(task);
        submit(callable, callback);
    }

    @Override
    public void submitToKeyOwner(Runnable task, Object key, ExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(task);
        submitToKeyOwner(callable, key, callback);
    }

    @Override
    public void submitToMember(Runnable task, Member member, ExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(task);
        submitToMember(callable, member, callback);
    }

    @Override
    public void submitToMembers(Runnable task, Collection<Member> members, MultiExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(task);
        submitToMembers(callable, members, callback);
    }

    @Override
    public void submitToAllMembers(Runnable task, MultiExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(task);
        submitToAllMembers(callable, callback);
    }

    private <T> void submitToPartitionOwner(Callable<T> task, ExecutionCallback<T> callback, int partitionId) {
        if (isShutdown()) {
            throw new RejectedExecutionException(getRejectionMessage());
        }
        NodeEngine nodeEngine = getNodeEngine();
        CallableTaskOperation op = new CallableTaskOperation(name, null, task);
        OperationService operationService = nodeEngine.getOperationService();
        operationService.createInvocationBuilder(DistributedExecutorService.SERVICE_NAME, op, partitionId)
                        .setCallback(new ExecutionCallbackAdapter(callback)).invoke();
    }

    @Override
    public <T> void submit(Callable<T> task, ExecutionCallback<T> callback) {
        int partitionId = getTaskPartitionId(task);
        submitToPartitionOwner(task, callback, partitionId);
    }

    @Override
    public <T> void submitToKeyOwner(Callable<T> task, Object key, ExecutionCallback<T> callback) {
        NodeEngine nodeEngine = getNodeEngine();
        submitToPartitionOwner(task, callback, nodeEngine.getPartitionService().getPartitionId(key));
    }

    public <T> void submitToMember(Callable<T> task, Member member, ExecutionCallback<T> callback) {
        if (isShutdown()) {
            throw new RejectedExecutionException(getRejectionMessage());
        }
        NodeEngine nodeEngine = getNodeEngine();
        MemberCallableTaskOperation op = new MemberCallableTaskOperation(name, null, task);
        OperationService operationService = nodeEngine.getOperationService();
        Address address = ((MemberImpl) member).getAddress();
        operationService.createInvocationBuilder(DistributedExecutorService.SERVICE_NAME, op, address)
                        .setCallback(new ExecutionCallbackAdapter(callback)).invoke();
    }

    private String getRejectionMessage() {
        return "ExecutorService[" + name + "] is shutdown! In order to create a new ExecutorService with name '" + name
                + "', you need to destroy current ExecutorService first!";
    }

    @Override
    public <T> void submitToMembers(Callable<T> task, Collection<Member> members, MultiExecutionCallback callback) {
        NodeEngine nodeEngine = getNodeEngine();
        ExecutionCallbackAdapterFactory executionCallbackFactory = new ExecutionCallbackAdapterFactory(nodeEngine, members,
                callback);

        for (Member member : members) {
            submitToMember(task, member, executionCallbackFactory.<T>callbackFor(member));
        }
    }

    @Override
    public <T> void submitToAllMembers(Callable<T> task, MultiExecutionCallback callback) {
        NodeEngine nodeEngine = getNodeEngine();
        submitToMembers(task, nodeEngine.getClusterService().getMembers(), callback);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        List<Future<T>> futures = new ArrayList<Future<T>>(tasks.size());
        List<Future<T>> result = new ArrayList<Future<T>>(tasks.size());
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
            result.add(new CompletedFuture<T>(getNodeEngine().getSerializationService(), value, getAsyncExecutor()));
        }
        return result;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        if (unit == null) {
            throw new NullPointerException("unit must not be null");
        }
        if (tasks == null) {
            throw new NullPointerException("tasks must not be null");
        }
        long timeoutNanos = unit.toNanos(timeout);
        List<Future<T>> futures = new ArrayList<Future<T>>(tasks.size());
        List<Future<T>> result = new ArrayList<Future<T>>(tasks.size());
        boolean done = true;
        try {
            for (Callable<T> task : tasks) {
                long start = System.nanoTime();
                int partitionId = getTaskPartitionId(task);
                futures.add(submitToPartitionOwner(task, partitionId, true));
                timeoutNanos -= System.nanoTime() - start;
                if (timeoutNanos <= 0L) {
                    for (Future<T> future : futures) {
                        result.add(future);
                    }
                    return result;
                }
            }
            for (int i = 0, size = futures.size(); i < size; i++) {
                long start = System.nanoTime();
                Object value;
                try {
                    Future<T> future = futures.get(i);
                    value = future.get(timeoutNanos, TimeUnit.NANOSECONDS);
                } catch (ExecutionException e) {
                    value = e;
                } catch (TimeoutException e) {
                    done = false;
                    for (int l = i; l < size; l++) {
                        Future<T> f = futures.get(i);
                        if (!f.isDone()) {
                            result.add(f);
                        } else {
                            Object v;
                            try {
                                v = f.get();
                            } catch (ExecutionException ex) {
                                v = ex;
                            }
                            result.add(new CompletedFuture<T>(getNodeEngine().getSerializationService(), v, getAsyncExecutor()));
                        }
                    }
                    break;
                }
                result.add(new CompletedFuture<T>(getNodeEngine().getSerializationService(), value, getAsyncExecutor()));
                timeoutNanos -= System.nanoTime() - start;
            }
        } catch (Throwable t) {
            logger.severe(t);
        } finally {
            if (!done) {
                cancelAll(result);
            }
            return result;
        }
    }

    private static <T> void cancelAll(List<Future<T>> result) {
        for (Future<T> aResult : result) {
            aResult.cancel(true);
        }
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

    @Override
    protected RuntimeException throwNotActiveException() {
        throw new RejectedExecutionException();
    }

    @Override
    public boolean isShutdown() {
        try {
            return getService().isShutdown(name);
        } catch (HazelcastInstanceNotActiveException e) {
            return true;
        }
    }

    @Override
    public boolean isTerminated() {
        return isShutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
        return false;
    }

    @Override
    public void shutdown() {
        NodeEngine nodeEngine = getNodeEngine();
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        OperationService operationService = nodeEngine.getOperationService();
        Collection<Future> calls = new LinkedList<Future>();

        for (MemberImpl member : members) {
            if (member.localMember()) {
                getService().shutdownExecutor(name);
            } else {
                Future f = submitShutdownOperation(operationService, member);
                calls.add(f);
            }
        }
        for (Future f : calls) {
            try {
                f.get(1, TimeUnit.SECONDS);
            } catch (Exception exception) {
                if (logger.isFinestEnabled()) {
                    logger.finest(exception);
                }
            }
        }
    }

    private Future submitShutdownOperation(OperationService operationService, MemberImpl member) {
        ShutdownOperation op = new ShutdownOperation(name);
        return operationService.invokeOnTarget(getServiceName(), op, member.getAddress());
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown();
        return Collections.emptyList();
    }

    @Override
    public LocalExecutorStats getLocalExecutorStats() {
        return getService().getLocalExecutorStats(name);
    }

    @Override
    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }

    private ExecutorService getAsyncExecutor() {
        return getNodeEngine().getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);
    }

    private List<Member> selectMembers(MemberSelector memberSelector) {
        if (memberSelector == null) {
            throw new IllegalArgumentException("memberSelector must not be null");
        }
        List<Member> selected = new ArrayList<Member>();
        Collection<MemberImpl> members = getNodeEngine().getClusterService().getMemberList();
        for (MemberImpl member : members) {
            if (memberSelector.select(member)) {
                selected.add(member);
            }
        }
        return selected;
    }

    @Override
    public String toString() {
        return "IExecutorService{" + "name='" + name + '\'' + '}';
    }
}
