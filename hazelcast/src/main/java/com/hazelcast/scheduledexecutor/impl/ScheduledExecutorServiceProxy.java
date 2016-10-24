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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.executor.impl.RunnableAdapter;
import com.hazelcast.mapreduce.impl.HashMapAdapter;
import com.hazelcast.nio.Address;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.scheduledexecutor.IdentifiedRunnable;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.operations.GetAllScheduledOperation;
import com.hazelcast.scheduledexecutor.impl.operations.ScheduleTaskOperation;
import com.hazelcast.scheduledexecutor.impl.operations.ShutdownOperation;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService.SERVICE_NAME;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.util.FutureUtil.logAllExceptions;
import static com.hazelcast.util.FutureUtil.returnWithDeadline;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.util.Preconditions.checkNotNull;

@SuppressWarnings("unchecked")
public class ScheduledExecutorServiceProxy
        extends AbstractDistributedObject<DistributedScheduledExecutorService>
        implements IScheduledExecutorService {

    private static final int GET_ALL_SCHEDULED_TIMEOUT = 10;

    private static final FutureUtil.ExceptionHandler WHILE_SHUTDOWN_EXCEPTION_HANDLER =
            logAllExceptions("Exception while ScheduledExecutor Service shutdown", Level.FINEST);

    private final String name;

    private final int partitionCount;

    private final Random partitionRandom = new Random();

    ScheduledExecutorServiceProxy(String name, NodeEngine nodeEngine,
                                         DistributedScheduledExecutorService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return DistributedScheduledExecutorService.SERVICE_NAME;
    }

    @Override
    public IScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return schedule(UuidUtil.newUnsecureUuidString(), command, delay, unit);
    }

    //TODO tkountis - Clean up async / sync ops

    @Override
    public IScheduledFuture schedule(String name, Runnable command, long delay, TimeUnit unit) {
        RunnableAdapter<?> callable = createRunnableAdapter(command);
        return schedule(name, callable, delay, unit);
    }

    @Override
    public <V> IScheduledFuture<V> schedule(Callable<V> command, long delay, TimeUnit unit) {
        return schedule(UuidUtil.newUnsecureUuidString(), command, delay, unit);
    }

    @Override
    public <V> IScheduledFuture<V> schedule(String name, Callable<V> command, long delay, TimeUnit unit) {
        TaskDefinition<V> definition = new TaskDefinition<V>(
                TaskDefinition.Type.SINGLE_RUN, name, command, delay, unit);

        return submitAsync(name, new ScheduleTaskOperation(getName(), definition));
    }

    @Override
    public IScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduleWithRepetition(UuidUtil.newUnsecureUuidString(), command, initialDelay, period, unit);
    }

    @Override
    public IScheduledFuture<?> scheduleWithRepetition(String name, Runnable command, long initialDelay, long period, TimeUnit unit) {
        RunnableAdapter<?> adapter = createRunnableAdapter(command);
        TaskDefinition definition = new TaskDefinition(
                TaskDefinition.Type.WITH_REPETITION, name, adapter, initialDelay, 0, period, unit);

        return submitAsync(name, new ScheduleTaskOperation(getName(), definition));
    }

    @Override
    public IScheduledFuture<?> scheduleOnMember(Runnable command, Member member, long delay, TimeUnit unit) {
        return scheduleOnMember(UuidUtil.newUnsecureUuidString(), command, member, delay, unit);
    }

    @Override
    public IScheduledFuture<?> scheduleOnMember(String name, Runnable command, Member member, long delay, TimeUnit unit) {
        RunnableAdapter<?> callable = createRunnableAdapter(command);
        return scheduleOnMember(name, callable, member, delay, unit);
    }

    @Override
    public <V> IScheduledFuture<V> scheduleOnMember(Callable<V> command, Member member, long delay, TimeUnit unit) {
        return scheduleOnMember(UuidUtil.newUnsecureUuidString(), command, member, delay, unit);
    }

    @Override
    public <V> IScheduledFuture<V> scheduleOnMember(String name, Callable<V> command, Member member, long delay, TimeUnit unit) {
        TaskDefinition<V> definition = new TaskDefinition<V>(
                TaskDefinition.Type.SINGLE_RUN, name, command, delay, unit);

        return submitOnMemberAsync(name, new ScheduleTaskOperation(getName(), definition), member);
    }

    @Override
    public IScheduledFuture<?> scheduleOnMemberWithRepetition(Runnable command, Member member, long initialDelay, long period,
                                                              TimeUnit unit) {
        return scheduleOnMemberWithRepetition(UuidUtil.newUnsecureUuidString(), command, member, initialDelay, period, unit);
    }

    @Override
    public IScheduledFuture<?> scheduleOnMemberWithRepetition(String name, Runnable command, Member member, long initialDelay,
                                                              long period, TimeUnit unit) {
        RunnableAdapter<?> adapter = createRunnableAdapter(command);
        TaskDefinition definition = new TaskDefinition(
                TaskDefinition.Type.WITH_REPETITION, name, adapter, initialDelay, period, unit);
        return submitOnMemberAsync(name, new ScheduleTaskOperation(getName(), definition), member);
    }

    @Override
    public IScheduledFuture<?> scheduleOnKeyOwner(Runnable command, Object key, long delay, TimeUnit unit) {
        return scheduleOnKeyOwner(UuidUtil.newUnsecureUuidString(), command, key, delay, unit);
    }

    @Override
    public IScheduledFuture<?> scheduleOnKeyOwner(String name, Runnable command, Object key, long delay, TimeUnit unit) {
        RunnableAdapter<?> callable = createRunnableAdapter(command);
        return scheduleOnKeyOwner(name, callable, key, delay, unit);
    }

    @Override
    public <V> IScheduledFuture<V> scheduleOnKeyOwner(Callable<V> command, Object key, long delay, TimeUnit unit) {
        return scheduleOnKeyOwner(UuidUtil.newUnsecureUuidString(), command, key, delay, unit);
    }

    @Override
    public <V> IScheduledFuture<V> scheduleOnKeyOwner(String name, Callable<V> command, Object key, long delay, TimeUnit unit) {
        int partitionId = getKeyPartitionId(key);
        TaskDefinition definition = new TaskDefinition(
                TaskDefinition.Type.SINGLE_RUN, name, command, delay, unit);
        return submitOnPartitionAsync(name, new ScheduleTaskOperation(getName(), definition), partitionId);
    }

    @Override
    public IScheduledFuture<?> scheduleOnKeyOwnerWithRepetition(Runnable command, Object key, long initialDelay, long period,
                                                                TimeUnit unit) {
        return scheduleOnKeyOwnerWithRepetition(UuidUtil.newUnsecureUuidString(), command, key, initialDelay, period, unit);
    }

    @Override
    public IScheduledFuture<?> scheduleOnKeyOwnerWithRepetition(String name, Runnable command, Object key, long initialDelay,
                                                                long period, TimeUnit unit) {
        int partitionId = getKeyPartitionId(key);
        RunnableAdapter<?> adapter = createRunnableAdapter(command);
        TaskDefinition definition = new TaskDefinition(
                TaskDefinition.Type.WITH_REPETITION, name, adapter, initialDelay, 0, period, unit);

        return submitOnPartitionAsync(name, new ScheduleTaskOperation(getName(), definition), partitionId);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnAllMembers(Runnable command, long delay, TimeUnit unit) {
        return scheduleOnAllMembers(UuidUtil.newUnsecureUuidString(), command, delay, unit);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnAllMembers(String name, Runnable command, long delay, TimeUnit unit) {
        RunnableAdapter callable = createRunnableAdapter(command);
        return scheduleOnAllMembers(name, callable, delay, unit);
    }

    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnAllMembers(Callable<V> command, long delay, TimeUnit unit) {
        return scheduleOnAllMembers(UuidUtil.newUnsecureUuidString(), command, delay, unit);
    }

    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnAllMembers(String name, Callable<V> command, long delay, TimeUnit unit) {
        Map<Member, IScheduledFuture<V>> futures = new HashMap<Member, IScheduledFuture<V>>();
        for (Member member : getNodeEngine().getClusterService().getMembers()) {
            TaskDefinition<V> definition = new TaskDefinition<V>(
                    TaskDefinition.Type.SINGLE_RUN, name, command, delay, unit);

            futures.put(member, (IScheduledFuture<V>)
                    submitOnMemberAsync(name, new ScheduleTaskOperation(getName(), definition), member));
        }

        return futures;
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnAllMembersWithRepetition(Runnable command, long initialDelay, long period,
                                                                               TimeUnit unit) {
        return scheduleOnAllMembersWithRepetition(UuidUtil.newUnsecureUuidString(), command, initialDelay, period, unit);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnAllMembersWithRepetition(String name, Runnable command, long initialDelay, long period,
                                                                               TimeUnit unit) {
        RunnableAdapter<?> adapter = createRunnableAdapter(command);
        Map<Member, IScheduledFuture<?>> futures = new HashMapAdapter<Member, IScheduledFuture<?>>();
        for (Member member : getNodeEngine().getClusterService().getMembers()) {
            TaskDefinition definition = new TaskDefinition(
                    TaskDefinition.Type.WITH_REPETITION, name, adapter, initialDelay, 0, period, unit);

            futures.put(member, submitOnMemberAsync(name, new ScheduleTaskOperation(getName(), definition), member));
        }

        return futures;
    }

    @Override
    public Map<Member, IScheduledFuture> scheduleOnMembers(Runnable command, Collection<Member> members, long delay,
                                                           TimeUnit unit) {
        return scheduleOnMembers(UuidUtil.newUnsecureUuidString(), command, members, delay, unit);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<Member, IScheduledFuture> scheduleOnMembers(String name, Runnable command, Collection<Member> members, long delay,
                                                           TimeUnit unit) {
        RunnableAdapter callable = createRunnableAdapter(command);
        return (Map<Member, IScheduledFuture>) scheduleOnMembers(name, callable, members, delay, unit);
    }

    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnMembers(Callable<V> command, Collection<Member> members, long delay, TimeUnit unit) {
        return scheduleOnMembers(UuidUtil.newUnsecureUuidString(), command, members, delay, unit);
    }

    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnMembers(String name, Callable<V> command, Collection<Member> members, long delay,
                                                                  TimeUnit unit) {
        Map<Member, IScheduledFuture<V>> futures = new HashMap<Member, IScheduledFuture<V>>();
        for (Member member : members) {
            TaskDefinition<V> definition = new TaskDefinition<V>(
                    TaskDefinition.Type.SINGLE_RUN, name, command, delay, unit);

            futures.put(member,
                    (IScheduledFuture<V>) submitOnMemberAsync(name,
                            new ScheduleTaskOperation(getName(), definition), member));
        }

        return futures;
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnMembersWithRepetition(Runnable command, Collection<Member> members,
                                                                            long initialDelay, long period, TimeUnit unit) {
        return scheduleOnMembersWithRepetition(UuidUtil.newUnsecureUuidString(), command, members, initialDelay, period, unit);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnMembersWithRepetition(String name, Runnable command, Collection<Member> members,
                                                                            long initialDelay, long period, TimeUnit unit) {
        RunnableAdapter<?> adapter = createRunnableAdapter(command);
        Map<Member, IScheduledFuture<?>> futures = new HashMapAdapter<Member, IScheduledFuture<?>>();
        for (Member member : members) {
            TaskDefinition definition = new TaskDefinition(
                    TaskDefinition.Type.WITH_REPETITION, name, adapter, initialDelay, 0, period, unit);

            futures.put(member, submitOnMemberAsync(name, new ScheduleTaskOperation(getName(), definition), member));
        }

        return futures;
    }

    @Override
    public IScheduledFuture<?> getScheduled(ScheduledTaskHandler handler) {
        ScheduledFutureProxy proxy = new ScheduledFutureProxy(handler);
        attachHazelcastInstance(proxy);
        return proxy;
    }

    @Override
    public Map<Member, List<IScheduledFuture<?>>> getAllScheduled() {
        //TODO tkountis - Consider extra flavour with Executor provided as arg
        ExecutionService service = getNodeEngine().getExecutionService();
        ManagedExecutorService executor = getNodeEngine().getExecutionService().getExecutor(ASYNC_EXECUTOR);

        //TODO tkountis - Consider extra flavour with timeout as arg
        final long timeout = GET_ALL_SCHEDULED_TIMEOUT;

        Future<Map<Member, List<IScheduledFuture<?>>>> future =
                executor.submit(new Callable<Map<Member, List<IScheduledFuture<?>>>>() {
            @Override
            public Map<Member, List<IScheduledFuture<?>>> call()
                    throws Exception {

                Map<Member, List<IScheduledFuture<?>>> tasks =
                        new LinkedHashMap<Member, List<IScheduledFuture<?>>>();

                List<Member> members = new ArrayList<Member>(getNodeEngine().getClusterService().getMembers());
                List<Future<List<ScheduledTaskHandler>>> futures = new ArrayList<Future<List<ScheduledTaskHandler>>>();
                for (Member member : members) {
                    Address address = member.getAddress();

                    ICompletableFuture<List<ScheduledTaskHandler>> future =
                            getOperationService().invokeOnTarget(getServiceName(), new GetAllScheduledOperation(), address);

                    futures.add(future);
                }

                // TODO unit test - returnWithDeadline returns a Collection, currently in the same order, but
                // there is no contract to guarantee so for the future.
                List<List<ScheduledTaskHandler>> resolvedFutures = new ArrayList<List<ScheduledTaskHandler>>(
                        returnWithDeadline(futures, timeout, TimeUnit.SECONDS));

                for (int i = 0; i < members.size(); i++) {
                    Member member = members.get(i);
                    List<ScheduledTaskHandler> handlers = resolvedFutures.get(i);
                    List<IScheduledFuture<?>> scheduledFutures = new ArrayList<IScheduledFuture<?>>();

                    for (ScheduledTaskHandler handler : handlers) {
                        ScheduledFutureProxy proxy = new ScheduledFutureProxy(handler);
                        attachHazelcastInstance(proxy);
                        scheduledFutures.add(proxy);
                    }

                    tasks.put(member, scheduledFutures);
                }

                return tasks;
            }
        });

        try {
            return future.get();
        } catch (InterruptedException e) {
            sneakyThrow(e);
        } catch (ExecutionException e) {
            sneakyThrow(e);
        }

        return null;
    }

    @Override
    public void shutdown() {
        NodeEngine nodeEngine = getNodeEngine();
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        OperationService operationService = nodeEngine.getOperationService();
        Collection<Future> calls = new LinkedList<Future>();

        for (Member member : members) {
            if (member.localMember()) {
                getService().shutdownExecutor(name);
            } else {
                Operation op = new ShutdownOperation(name);
                Future f = operationService.invokeOnTarget(SERVICE_NAME, op, member.getAddress());
                calls.add(f);
            }
        }

        waitWithDeadline(calls, 1, TimeUnit.SECONDS, WHILE_SHUTDOWN_EXCEPTION_HANDLER);
    }

    @Override
    public List<IdentifiedRunnable> shutdownNow() {
        return null;
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    private <T> RunnableAdapter<T> createRunnableAdapter(Runnable command) {
        checkNotNull(command, "Command can't be null");

        return new RunnableAdapter<T>(command);
    }

    //TODO tkountis - Add support for PartitionAware tasks
    private int getKeyPartitionId(Object key) {
        return getNodeEngine().getPartitionService().getPartitionId(key);
    }

    private <V> IScheduledFuture<V> submitAsync(String taskName, Operation op) {
        int partitionId = partitionRandom.nextInt(partitionCount);
        return submitOnPartitionAsync(taskName, op, partitionId);
    }

    private <V> IScheduledFuture<V> submitOnPartitionAsync(String taskName, Operation op, int partitionId) {
        op.setPartitionId(partitionId);
        invokeOnPartition(op);

        ScheduledFutureProxy futureProxy = new ScheduledFutureProxy(
                ScheduledTaskHandler.of(partitionId, getName(), taskName));
        attachHazelcastInstance(futureProxy);
        return futureProxy;
    }

    private <V> IScheduledFuture<V> submitOnMemberAsync(String taskName, Operation op, Member member) {
        Address address = member.getAddress();
        op.setPartitionId(-1); // Make partition agnostic

        getOperationService().invokeOnTarget(getServiceName(), op, address);

        ScheduledFutureProxy futureProxy = new ScheduledFutureProxy(
                ScheduledTaskHandler.of(address, getName(), taskName));
        attachHazelcastInstance(futureProxy);
        return futureProxy;
    }

    private void attachHazelcastInstance(HazelcastInstanceAware future) {
        future.setHazelcastInstance(getNodeEngine().getHazelcastInstance());
    }

}
