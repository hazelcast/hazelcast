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

package com.hazelcast.executor.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.executor.LocalExecutorStats;
import com.hazelcast.executor.impl.operations.CallableTaskOperation;
import com.hazelcast.executor.impl.operations.MemberCallableTaskOperation;
import com.hazelcast.executor.impl.operations.ShutdownOperation;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.FutureUtil.ExceptionHandler;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;

@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public class ExecutorServiceProxy
        extends AbstractDistributedObject<DistributedExecutorService>
        implements IExecutorService {

    private final ExceptionHandler shutdownExceptionHandler = new ExceptionHandler() {
        @Override
        public void handleException(Throwable throwable) {
            if (throwable != null) {
                if (throwable instanceof SplitBrainProtectionException) {
                    sneakyThrow(throwable);
                }
                if (throwable.getCause() instanceof SplitBrainProtectionException) {
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

    public ExecutorServiceProxy(String name, NodeEngine nodeEngine, DistributedExecutorService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.logger = nodeEngine.getLogger(ExecutorServiceProxy.class);
        getLocalExecutorStats();
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
    public void execute(@Nonnull Runnable command) {
        Callable<?> callable = createRunnableAdapter(command);
        submit(callable);
    }

    private <T> RunnableAdapter<T> createRunnableAdapter(@Nonnull Runnable command) {
        checkNotNull(command, "Command must not be null");

        return new RunnableAdapter<>(command);
    }

    @Override
    public void executeOnKeyOwner(@Nonnull Runnable command,
                                  @Nonnull Object key) {
        Callable<?> callable = createRunnableAdapter(command);
        submitToKeyOwner(callable, key);
    }

    @Override
    public void executeOnMember(@Nonnull Runnable command,
                                @Nonnull Member member) {
        Callable<?> callable = createRunnableAdapter(command);
        submitToMember(callable, member);
    }

    @Override
    public void executeOnMembers(@Nonnull Runnable command,
                                 @Nonnull Collection<Member> members) {
        Callable<?> callable = createRunnableAdapter(command);
        submitToMembers(callable, members);
    }

    @Override
    public void executeOnAllMembers(@Nonnull Runnable command) {
        Callable<?> callable = createRunnableAdapter(command);
        submitToAllMembers(callable);
    }

    @Nonnull
    @Override
    public Future<?> submit(@Nonnull Runnable task) {
        Callable<?> callable = createRunnableAdapter(task);
        return submit(callable);
    }

    @Nonnull
    @Override
    public <T> Future<T> submit(@Nonnull Runnable task, T result) {
        checkNotNull(task, "task must not be null");
        checkNotShutdown();

        NodeEngine nodeEngine = getNodeEngine();
        Callable<T> callable = createRunnableAdapter(task);
        Data callableData = nodeEngine.toData(callable);
        UUID uuid = newUnsecureUUID();
        int partitionId = getTaskPartitionId(callable);

        Operation op = new CallableTaskOperation(name, uuid, callableData)
                .setPartitionId(partitionId);
        InvocationFuture future = invokeOnPartition(op);
        return new CancellableDelegatingFuture<>(future, result, nodeEngine, uuid, partitionId);
    }

    private void checkNotShutdown() {
        if (isShutdown()) {
            throw new RejectedExecutionException(getRejectionMessage());
        }
    }

    @Nonnull
    @Override
    public <T> Future<T> submit(@Nonnull Callable<T> task) {
        checkNotNull(task, "task must not be null");
        final int partitionId = getTaskPartitionId(task);
        return submitToPartitionOwner(task, partitionId);
    }

    private @Nonnull
    <T> Future<T> submitToPartitionOwner(@Nonnull Callable<T> task,
                                         int partitionId) {
        checkNotNull(task, "task must not be null");
        checkNotShutdown();

        NodeEngine nodeEngine = getNodeEngine();
        Data taskData = nodeEngine.toData(task);
        UUID uuid = newUnsecureUUID();

        Operation op = new CallableTaskOperation(name, uuid, taskData)
                .setPartitionId(partitionId);
        InternalCompletableFuture future = invokeOnPartition(op);
        return new CancellableDelegatingFuture<>(future, nodeEngine, uuid, partitionId);
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
    public <T> Future<T> submitToKeyOwner(@Nonnull Callable<T> task,
                                          @Nonnull Object key) {
        checkNotNull(key, "key must not be null");
        NodeEngine nodeEngine = getNodeEngine();
        return submitToPartitionOwner(task, nodeEngine.getPartitionService().getPartitionId(key));
    }

    @Override
    public <T> Future<T> submitToMember(@Nonnull Callable<T> task,
                                        @Nonnull Member member) {
        checkNotNull(task, "task must not be null");
        checkNotNull(member, "member must not be null");
        checkNotShutdown();

        Data taskData = getNodeEngine().toData(task);
        return submitToMember(taskData, member);
    }

    private <T> Future<T> submitToMember(@Nonnull Data taskData,
                                         @Nonnull Member member) {
        NodeEngine nodeEngine = getNodeEngine();
        UUID uuid = newUnsecureUUID();
        Address target = member.getAddress();

        MemberCallableTaskOperation op = new MemberCallableTaskOperation(name, uuid, taskData);
        InternalCompletableFuture future = nodeEngine.getOperationService()
                .invokeOnTarget(DistributedExecutorService.SERVICE_NAME, op, target);
        return new CancellableDelegatingFuture<>(future, nodeEngine, uuid, target);
    }

    @Override
    public <T> Map<Member, Future<T>> submitToMembers(@Nonnull Callable<T> task,
                                                      @Nonnull Collection<Member> members) {
        checkNotNull(task, "task must not be null");
        checkNotNull(members, "members must not be null");
        checkNotShutdown();
        Data taskData = getNodeEngine().toData(task);
        Map<Member, Future<T>> futures = createHashMap(members.size());
        for (Member member : members) {
            futures.put(member, submitToMember(taskData, member));
        }
        return futures;
    }

    @Override
    public <T> Map<Member, Future<T>> submitToAllMembers(@Nonnull Callable<T> task) {
        NodeEngine nodeEngine = getNodeEngine();
        return submitToMembers(task, nodeEngine.getClusterService().getMembers());
    }

    @Override
    public void submit(@Nonnull Runnable task,
                       @Nullable ExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(task);
        submit(callable, callback);
    }

    @Override
    public void submitToKeyOwner(@Nonnull Runnable task,
                                 @Nonnull Object key,
                                 @Nonnull ExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(task);
        submitToKeyOwner(callable, key, callback);
    }

    @Override
    public void submitToMember(@Nonnull Runnable task,
                               @Nonnull Member member,
                               @Nullable ExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(task);
        submitToMember(callable, member, callback);
    }

    @Override
    public void submitToMembers(@Nonnull Runnable task,
                                @Nonnull Collection<Member> members,
                                @Nonnull MultiExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(task);
        submitToMembers(callable, members, callback);
    }

    @Override
    public void submitToAllMembers(@Nonnull Runnable task,
                                   @Nonnull MultiExecutionCallback callback) {
        Callable<?> callable = createRunnableAdapter(task);
        submitToAllMembers(callable, callback);
    }

    private <T> void submitToPartitionOwner(@Nonnull Callable<T> task,
                                            @Nullable ExecutionCallback<T> callback,
                                            int partitionId) {
        checkNotShutdown();
        checkNotNull(task, "task must not be null");

        NodeEngine nodeEngine = getNodeEngine();
        Data taskData = nodeEngine.toData(task);
        CallableTaskOperation op = new CallableTaskOperation(name, null, taskData);
        OperationService operationService = nodeEngine.getOperationService();
        InvocationFuture<T> future = operationService
                .createInvocationBuilder(DistributedExecutorService.SERVICE_NAME, op, partitionId)
                .invoke();
        if (callback != null) {
            future.whenCompleteAsync(new ExecutionCallbackAdapter<>(callback))
                    .whenCompleteAsync((v, t) -> {
                        if (t instanceof RejectedExecutionException) {
                            callback.onFailure(t);
                        }
                    });
        }
    }

    @Override
    public <T> void submit(@Nonnull Callable<T> task,
                           @Nullable ExecutionCallback<T> callback) {
        int partitionId = getTaskPartitionId(task);
        submitToPartitionOwner(task, callback, partitionId);
    }

    @Override
    public <T> void submitToKeyOwner(@Nonnull Callable<T> task,
                                     @Nonnull Object key,
                                     @Nullable ExecutionCallback<T> callback) {
        checkNotNull(key, "key must not be null");
        checkNotNull(task, "task must not be null");

        NodeEngine nodeEngine = getNodeEngine();
        submitToPartitionOwner(task, callback, nodeEngine.getPartitionService().getPartitionId(key));
    }

    private <T> void submitToMember(@Nonnull Data taskData,
                                    @Nonnull Member member,
                                    @Nullable ExecutionCallback<T> callback) {
        checkNotNull(member, "member must not be null");
        checkNotShutdown();

        NodeEngine nodeEngine = getNodeEngine();
        UUID uuid = newUnsecureUUID();
        MemberCallableTaskOperation op = new MemberCallableTaskOperation(name, uuid, taskData);
        OperationService operationService = nodeEngine.getOperationService();
        Address address = member.getAddress();
        InvocationFuture<T> future = operationService
                .createInvocationBuilder(DistributedExecutorService.SERVICE_NAME, op, address)
                .invoke();
        if (callback != null) {
            future.whenCompleteAsync(new ExecutionCallbackAdapter<>(callback))
                    .whenCompleteAsync((v, t) -> {
                        if (t instanceof RejectedExecutionException) {
                            callback.onFailure(t);
                        }
                    });
        }
    }

    @Override
    public <T> void submitToMember(@Nonnull Callable<T> task,
                                   @Nonnull Member member,
                                   @Nullable ExecutionCallback<T> callback) {
        checkNotNull(task, "task must not be null");
        checkNotShutdown();

        Data taskData = getNodeEngine().toData(task);
        submitToMember(taskData, member, callback);
    }

    private String getRejectionMessage() {
        return "ExecutorService[" + name + "] is shutdown! In order to create a new ExecutorService with name '" + name
                + "', you need to destroy current ExecutorService first!";
    }

    @Override
    public <T> void submitToMembers(@Nonnull Callable<T> task,
                                    @Nonnull Collection<Member> members,
                                    @Nonnull MultiExecutionCallback callback) {
        checkNotNull(task, "task must not be null");
        checkNotNull(members, "members must not be null");
        NodeEngine nodeEngine = getNodeEngine();
        ILogger logger = nodeEngine.getLogger(ExecutionCallbackAdapterFactory.class);
        ExecutionCallbackAdapterFactory executionCallbackFactory = new ExecutionCallbackAdapterFactory(logger, members, callback);

        Data taskData = nodeEngine.toData(task);
        for (Member member : members) {
            submitToMember(taskData, member, executionCallbackFactory.<T>callbackFor(member));
        }
    }

    @Override
    public <T> void submitToAllMembers(@Nonnull Callable<T> task,
                                       @Nonnull MultiExecutionCallback callback) {
        NodeEngine nodeEngine = getNodeEngine();
        submitToMembers(task, nodeEngine.getClusterService().getMembers(), callback);
    }

    @Nonnull
    @Override
    public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        checkNotNull(tasks, "tasks must not be null");
        List<Future<T>> futures = new ArrayList<>(tasks.size());
        List<Future<T>> result = new ArrayList<>(tasks.size());
        for (Callable<T> task : tasks) {
            futures.add(submit(task));
        }
        for (Future<T> future : futures) {
            result.add(completedSynchronously(future, getNodeEngine().getSerializationService()));
        }
        return result;
    }

    @Nonnull
    @Override
    public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks,
                                         long timeout,
                                         @Nonnull TimeUnit unit) {
        checkNotNull(unit, "unit must not be null");
        checkNotNull(tasks, "tasks must not be null");

        long timeoutNanos = unit.toNanos(timeout);
        List<Future<T>> futures = new ArrayList<>(tasks.size());
        boolean done = false;
        try {
            for (Callable<T> task : tasks) {
                long startNanos = Timer.nanos();
                int partitionId = getTaskPartitionId(task);
                futures.add(submitToPartitionOwner(task, partitionId));
                timeoutNanos -= Timer.nanosElapsed(startNanos);
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
            long startNanos = Timer.nanos();
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
                        futures.set(l, completedSynchronously(f, getNodeEngine().getSerializationService()));
                    }
                }
                break;
            }

            futures.set(i, InternalCompletableFuture.newCompletedFuture(value));
            timeoutNanos -= Timer.nanosElapsed(startNanos);
        }
        return done;
    }

    private static <T> void cancelAll(List<Future<T>> result) {
        for (Future<T> aResult : result) {
            aResult.cancel(true);
        }
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
    public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) {
        checkNotNull(unit, "unit must not be null");
        return false;
    }

    @Override
    public void shutdown() {
        NodeEngine nodeEngine = getNodeEngine();
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        OperationService operationService = nodeEngine.getOperationService();
        Collection<Future> calls = new LinkedList<>();

        for (Member member : members) {
            Future f = submitShutdownOperation(operationService, member);
            calls.add(f);
        }
        waitWithDeadline(calls, 3, TimeUnit.SECONDS, shutdownExceptionHandler);
    }

    private InvocationFuture<Object> submitShutdownOperation(OperationService operationService, Member member) {
        ShutdownOperation op = new ShutdownOperation(name);
        return operationService.invokeOnTarget(getServiceName(), op, member.getAddress());
    }

    @Nonnull
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

    private List<Member> selectMembers(@Nonnull MemberSelector memberSelector) {
        checkNotNull(memberSelector, "memberSelector must not be null");
        List<Member> selected = new ArrayList<>();
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

    private static <V> InternalCompletableFuture<V> completedSynchronously(Future<V> future,
                                                                           SerializationService serializationService) {
        try {
            return InternalCompletableFuture.newCompletedFuture(future.get(), serializationService);
        } catch (ExecutionException e) {
            return InternalCompletableFuture.completedExceptionally(e.getCause() == null ? e : e.getCause());
        } catch (CancellationException e) {
            InternalCompletableFuture cancelledFuture = new InternalCompletableFuture();
            future.cancel(true);
            return cancelledFuture;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return InternalCompletableFuture.completedExceptionally(e);
        }
    }
}
