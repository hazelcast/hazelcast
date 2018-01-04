/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.executor.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.executor.impl.operations.CallableTaskOperation;
import com.hazelcast.executor.impl.operations.MemberCallableTaskOperation;
import com.hazelcast.executor.impl.operations.ShutdownOperation;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.FutureUtil.ExceptionHandler;
import com.hazelcast.util.executor.CompletedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import java.util.logging.Level;

import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.UuidUtil.newUnsecureUuidString;

public class ExecutorServiceProxy
        extends AbstractDistributedObject<DistributedExecutorService>
        implements IExecutorService {

    public static final int SYNC_FREQUENCY = 100;
    public static final int SYNC_DELAY_MS = 10;

    private static final AtomicIntegerFieldUpdater<ExecutorServiceProxy> CONSECUTIVE_SUBMITS = AtomicIntegerFieldUpdater
            .newUpdater(ExecutorServiceProxy.class, "consecutiveSubmits");

    private final ExceptionHandler shutdownExceptionHandler = new ExceptionHandler() {
        @Override
        public void handleException(Throwable throwable) {
            if (throwable != null) {
                if (throwable instanceof QuorumException) {
                    sneakyThrow(throwable);
                }
                if (throwable.getCause() instanceof QuorumException) {
                    sneakyThrow(throwable.getCause());
                }
            }
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, "Exception while ExecutorService shutdown", throwable);
            }
        }
    };

    private final String name;
    private final Random random = new Random(-System.currentTimeMillis());
    private final int partitionCount;
    private final ILogger logger;

    // This field is never accessed directly but by the CONSECUTIVE_SUBMITS above
    private volatile int consecutiveSubmits;

    private volatile long lastSubmitTime;

    public ExecutorServiceProxy(String name, NodeEngine nodeEngine, DistributedExecutorService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.logger = nodeEngine.getLogger(ExecutorServiceProxy.class);
        getLocalExecutorStats();
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
        checkNotNull(command, "Command can't be null");

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
        checkNotNull(task, "task can't be null");
        checkNotShutdown();

        NodeEngine nodeEngine = getNodeEngine();
        Callable<T> callable = createRunnableAdapter(task);
        Data callableData = nodeEngine.toData(callable);
        String uuid = newUnsecureUuidString();
        int partitionId = getTaskPartitionId(callable);

        Operation op = new CallableTaskOperation(name, uuid, callableData)
                .setPartitionId(partitionId);
        InternalCompletableFuture future = invokeOnPartition(op);
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

    private void checkNotShutdown() {
        if (isShutdown()) {
            throw new RejectedExecutionException(getRejectionMessage());
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        final int partitionId = getTaskPartitionId(task);
        return submitToPartitionOwner(task, partitionId, false);
    }

    private <T> Future<T> submitToPartitionOwner(Callable<T> task, int partitionId, boolean preventSync) {
        checkNotNull(task, "task can't be null");
        checkNotShutdown();

        NodeEngine nodeEngine = getNodeEngine();
        Data taskData = nodeEngine.toData(task);
        String uuid = newUnsecureUuidString();

        boolean sync = !preventSync && checkSync();
        Operation op = new CallableTaskOperation(name, uuid, taskData)
                .setPartitionId(partitionId);
        InternalCompletableFuture future = invokeOnPartition(op);
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
        if (last + SYNC_DELAY_MS < now) {
            CONSECUTIVE_SUBMITS.set(this, 0);
        } else if (CONSECUTIVE_SUBMITS.incrementAndGet(this) % SYNC_FREQUENCY == 0) {
            sync = true;
        }
        lastSubmitTime = now;
        return sync;
    }

    private <T> int getTaskPartitionId(Callable<T> task) {
        if (task instanceof PartitionAware) {
            Object partitionKey = ((PartitionAware) task).getPartitionKey();
            if (partitionKey != null) {
                return getNodeEngine().getPartitionService().getPartitionId(partitionKey);
            }
        }
        return random.nextInt(partitionCount);
    }

    @Override
    public <T> Future<T> submitToKeyOwner(Callable<T> task, Object key) {
        NodeEngine nodeEngine = getNodeEngine();
        return submitToPartitionOwner(task, nodeEngine.getPartitionService().getPartitionId(key), false);
    }

    @Override
    public <T> Future<T> submitToMember(Callable<T> task, Member member) {
        checkNotNull(task, "task can't be null");
        checkNotShutdown();

        NodeEngine nodeEngine = getNodeEngine();
        Data taskData = nodeEngine.toData(task);
        String uuid = newUnsecureUuidString();
        Address target = ((MemberImpl) member).getAddress();

        boolean sync = checkSync();
        MemberCallableTaskOperation op = new MemberCallableTaskOperation(name, uuid, taskData);
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
        Map<Member, Future<T>> futures = createHashMap(members.size());
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
        checkNotShutdown();

        NodeEngine nodeEngine = getNodeEngine();
        Data taskData = nodeEngine.toData(task);
        CallableTaskOperation op = new CallableTaskOperation(name, null, taskData);
        OperationService operationService = nodeEngine.getOperationService();
        operationService.createInvocationBuilder(DistributedExecutorService.SERVICE_NAME, op, partitionId)
                .setExecutionCallback((ExecutionCallback) callback).invoke();
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

    @Override
    public <T> void submitToMember(Callable<T> task, Member member, ExecutionCallback<T> callback) {
        checkNotShutdown();

        NodeEngine nodeEngine = getNodeEngine();
        Data taskData = nodeEngine.toData(task);
        String uuid = newUnsecureUuidString();
        MemberCallableTaskOperation op = new MemberCallableTaskOperation(name, uuid, taskData);
        OperationService operationService = nodeEngine.getOperationService();
        Address address = ((MemberImpl) member).getAddress();
        operationService
                .createInvocationBuilder(DistributedExecutorService.SERVICE_NAME, op, address)
                .setExecutionCallback((ExecutionCallback) callback).invoke();
    }

    private String getRejectionMessage() {
        return "ExecutorService[" + name + "] is shutdown! In order to create a new ExecutorService with name '" + name
                + "', you need to destroy current ExecutorService first!";
    }

    @Override
    public <T> void submitToMembers(Callable<T> task, Collection<Member> members, MultiExecutionCallback callback) {
        NodeEngine nodeEngine = getNodeEngine();
        ExecutionCallbackAdapterFactory executionCallbackFactory = new ExecutionCallbackAdapterFactory(
                nodeEngine.getLogger(ExecutionCallbackAdapterFactory.class), members, callback);

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
        checkNotNull(unit, "unit must not be null");
        checkNotNull(tasks, "tasks must not be null");

        long timeoutNanos = unit.toNanos(timeout);
        List<Future<T>> futures = new ArrayList<Future<T>>(tasks.size());
        boolean done = false;
        try {
            for (Callable<T> task : tasks) {
                long start = System.nanoTime();
                int partitionId = getTaskPartitionId(task);
                futures.add(submitToPartitionOwner(task, partitionId, true));
                timeoutNanos -= System.nanoTime() - start;
            }
            if (timeoutNanos <= 0L) {
                return futures;
            }

            done = wait(timeoutNanos, futures);
            return futures;
        } catch (Throwable t) {
            logger.severe(t);
            // todo: should an exception not be thrown?
            return futures;
        } finally {
            if (!done) {
                cancelAll(futures);
            }
        }
    }

    private <T> boolean wait(long timeoutNanos, List<Future<T>> futures) throws InterruptedException {
        boolean done = true;
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
                    if (f.isDone()) {
                        Object v;
                        try {
                            v = f.get();
                        } catch (ExecutionException ex) {
                            v = ex;
                        }
                        futures.set(l, new CompletedFuture<T>(getNodeEngine().getSerializationService(), v, getAsyncExecutor()));
                    }
                }
                break;
            }

            futures.set(i, new CompletedFuture<T>(getNodeEngine().getSerializationService(), value, getAsyncExecutor()));
            timeoutNanos -= System.nanoTime() - start;
        }
        return done;
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
    protected void throwNotActiveException() {
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
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        OperationService operationService = nodeEngine.getOperationService();
        Collection<Future> calls = new LinkedList<Future>();

        for (Member member : members) {
            Future f = submitShutdownOperation(operationService, member);
            calls.add(f);
        }
        waitWithDeadline(calls, 3, TimeUnit.SECONDS, shutdownExceptionHandler);
    }

    private InternalCompletableFuture submitShutdownOperation(OperationService operationService, Member member) {
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
        Collection<Member> members = getNodeEngine().getClusterService().getMembers();
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

    @Override
    public String toString() {
        return "IExecutorService{" + "name='" + name + '\'' + '}';
    }
}
