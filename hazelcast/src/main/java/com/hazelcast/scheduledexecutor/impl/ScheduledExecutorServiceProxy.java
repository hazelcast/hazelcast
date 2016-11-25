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
import com.hazelcast.core.PartitionAware;
import com.hazelcast.mapreduce.impl.HashMapAdapter;
import com.hazelcast.nio.Address;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.scheduledexecutor.NamedTask;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.operations.GetAllScheduledOperation;
import com.hazelcast.scheduledexecutor.impl.operations.ScheduleTaskOperation;
import com.hazelcast.scheduledexecutor.impl.operations.ShutdownOperation;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
    public IScheduledFuture schedule(Runnable command, long delay, TimeUnit unit) {
        ScheduledRunnableAdapter<?> callable = createScheduledRunnableAdapter(command);
        return schedule(callable, delay, unit);
    }

    @Override
    public <V> IScheduledFuture<V> schedule(Callable<V> command, long delay, TimeUnit unit) {
        String name = extractNameOrGenerateOne(command);
        int partitionId = getTaskOrKeyPartitionId(command, name);

        TaskDefinition<V> definition = new TaskDefinition<V>(
                TaskDefinition.Type.SINGLE_RUN, name, command, delay, unit);

        return submitOnPartitionSync(name, new ScheduleTaskOperation(getName(), definition), partitionId);
    }

    @Override
    public IScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay, long period, TimeUnit unit) {
        String name = extractNameOrGenerateOne(command);
        int partitionId = getTaskOrKeyPartitionId(command, name);

        ScheduledRunnableAdapter<?> adapter = createScheduledRunnableAdapter(command);
        TaskDefinition definition = new TaskDefinition(
                TaskDefinition.Type.WITH_REPETITION, name, adapter, initialDelay, 0, period, unit);

        return submitOnPartitionSync(name, new ScheduleTaskOperation(getName(), definition), partitionId);
    }

    @Override
    public IScheduledFuture<?> scheduleOnMember(Runnable command, Member member, long delay, TimeUnit unit) {
        return scheduleOnMembers(command, Collections.singleton(member), delay, unit).get(member);
    }

    @Override
    public <V> IScheduledFuture<V> scheduleOnMember(Callable<V> command, Member member, long delay, TimeUnit unit) {
        return scheduleOnMembers(command, Collections.singleton(member), delay, unit).get(member);
    }

    @Override
    public IScheduledFuture<?> scheduleOnMemberWithRepetition(Runnable command, Member member, long initialDelay,
                                                              long period, TimeUnit unit) {
        return scheduleOnMembersWithRepetition(command, Collections.singleton(member), initialDelay, period, unit).get(member);
    }

    @Override
    public IScheduledFuture<?> scheduleOnKeyOwner(Runnable command, Object key, long delay, TimeUnit unit) {
        ScheduledRunnableAdapter<?> callable = createScheduledRunnableAdapter(command);
        return scheduleOnKeyOwner(callable, key, delay, unit);
    }

    @Override
    public <V> IScheduledFuture<V> scheduleOnKeyOwner(Callable<V> command, Object key, long delay, TimeUnit unit) {
        String name = extractNameOrGenerateOne(command);
        int partitionId = getKeyPartitionId(key);

        TaskDefinition definition = new TaskDefinition(
                TaskDefinition.Type.SINGLE_RUN, name, command, delay, unit);
        return submitOnPartitionSync(name, new ScheduleTaskOperation(getName(), definition), partitionId);
    }

    @Override
    public IScheduledFuture<?> scheduleOnKeyOwnerWithRepetition(Runnable command, Object key, long initialDelay,
                                                                long period, TimeUnit unit) {
        String name = extractNameOrGenerateOne(command);
        int partitionId = getKeyPartitionId(key);
        ScheduledRunnableAdapter<?> adapter = createScheduledRunnableAdapter(command);
        TaskDefinition definition = new TaskDefinition(
                TaskDefinition.Type.WITH_REPETITION, name, adapter, initialDelay, 0, period, unit);

        return submitOnPartitionSync(name, new ScheduleTaskOperation(getName(), definition), partitionId);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnAllMembers(Runnable command, long delay, TimeUnit unit) {
        return scheduleOnMembers(command, getNodeEngine().getClusterService().getMembers(), delay, unit);
    }

    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnAllMembers(Callable<V> command, long delay, TimeUnit unit) {
        return scheduleOnMembers(command, getNodeEngine().getClusterService().getMembers(), delay, unit);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnAllMembersWithRepetition(Runnable command, long initialDelay, long period,
                                                                               TimeUnit unit) {
       return scheduleOnMembersWithRepetition(command, getNodeEngine().getClusterService().getMembers(),
               initialDelay, period, unit);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnMembers(Runnable command, Collection<Member> members, long delay,
                                                              TimeUnit unit) {
        ScheduledRunnableAdapter callable = createScheduledRunnableAdapter(command);
        return (Map<Member, IScheduledFuture<?>>) scheduleOnMembers(callable, members, delay, unit);
    }

    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnMembers(Callable<V> command, Collection<Member> members, long delay,
                                                                  TimeUnit unit) {
        String name = extractNameOrGenerateOne(command);
        Map<Member, IScheduledFuture<V>> futures = new HashMap<Member, IScheduledFuture<V>>();
        for (Member member : members) {
            TaskDefinition<V> definition = new TaskDefinition<V>(
                    TaskDefinition.Type.SINGLE_RUN, name, command, delay, unit);

            futures.put(member,
                    (IScheduledFuture<V>) submitOnMemberSync(name,
                            new ScheduleTaskOperation(getName(), definition), member));
        }

        return futures;
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnMembersWithRepetition(Runnable command, Collection<Member> members,
                                                                            long initialDelay, long period, TimeUnit unit) {
        String name = extractNameOrGenerateOne(command);
        ScheduledRunnableAdapter<?> adapter = createScheduledRunnableAdapter(command);
        Map<Member, IScheduledFuture<?>> futures = new HashMapAdapter<Member, IScheduledFuture<?>>();
        for (Member member : members) {
            TaskDefinition definition = new TaskDefinition(
                    TaskDefinition.Type.WITH_REPETITION, name, adapter, initialDelay, 0, period, unit);

            futures.put(member, submitOnMemberSync(name, new ScheduleTaskOperation(getName(), definition), member));
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
                            getOperationService().invokeOnTarget(getServiceName(), new GetAllScheduledOperation(getName()), address);

                    futures.add(future);
                }

                // TODO unit test - returnWithDeadline returns a Collection, currently in the same order, but
                // there is no contract to guarantee so for the future.
                List<List<ScheduledTaskHandler>> resolvedFutures = new ArrayList<List<ScheduledTaskHandler>>(
                        returnWithDeadline(futures, timeout, TimeUnit.SECONDS));

                for (int i = 0; i < resolvedFutures.size(); i++) {
                    Member member = members.get(i);
                    List<ScheduledTaskHandler> handlers = resolvedFutures.get(i);
                    List<IScheduledFuture<?>> scheduledFutures = new ArrayList<IScheduledFuture<?>>();

                    for (ScheduledTaskHandler handler : handlers) {
                        ScheduledFutureProxy proxy = new ScheduledFutureProxy(handler);
                        attachHazelcastInstance(proxy);
                        scheduledFutures.add(proxy);
                    }

                    if (!scheduledFutures.isEmpty()) {
                        tasks.put(member, scheduledFutures);
                    }
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

    private String extractNameOrGenerateOne(Object command) {
        String name = null;
        if (command instanceof NamedTask) {
            name = ((NamedTask) command).getName();
        }

        return name != null ? name : UuidUtil.newUnsecureUuidString();
    }

    private <T> ScheduledRunnableAdapter<T> createScheduledRunnableAdapter(Runnable command) {
        checkNotNull(command, "Command can't be null");

        return new ScheduledRunnableAdapter<T>(command);
    }

    private <V> IScheduledFuture<V> createFutureProxy(int partitionId, String taskName) {
        ScheduledFutureProxy proxy = new ScheduledFutureProxy(
                ScheduledTaskHandler.of(partitionId, getName(), taskName));
        proxy.setHazelcastInstance(getNodeEngine().getHazelcastInstance());
        return proxy;
    }

    private <V> IScheduledFuture<V> createFutureProxy(Address address, String taskName) {
        ScheduledFutureProxy proxy = new ScheduledFutureProxy(
                ScheduledTaskHandler.of(address, getName(), taskName));
        proxy.setHazelcastInstance(getNodeEngine().getHazelcastInstance());
        return proxy;
    }

    private int getKeyPartitionId(Object key) {
        return getNodeEngine().getPartitionService().getPartitionId(key);
    }

    private int getTaskOrKeyPartitionId(Callable task, Object key) {
        if (task instanceof PartitionAware) {
            Object newKey = ((PartitionAware) task).getPartitionKey();
            if (newKey != null) {
                key = newKey;
            }
        }

        return getKeyPartitionId(key);
    }

    private int getTaskOrKeyPartitionId(Runnable task, Object key) {
        if (task instanceof PartitionAware) {
            key = ((PartitionAware) task).getPartitionKey();
        }

        return getKeyPartitionId(key);
    }

    // TODO tkountis - Document that OPs are sync to avoid race-conditions;
    // Member OPs since they are not forced to run in the partition thread
    //  I can trigger a schedule and then GetAll and its not there yet
    // Also partition based ones, if Node goes down before backup completes also failing.
    private <V> IScheduledFuture<V> submitOnPartitionSync(String taskName, Operation op, int partitionId) {
        op.setPartitionId(partitionId);
        invokeOnPartition(op).join();
        return createFutureProxy(partitionId, taskName);
    }


    private <V> IScheduledFuture<V> submitOnMemberSync(String taskName, Operation op, Member member) {
        Address address = member.getAddress();
        getOperationService().invokeOnTarget(getServiceName(), op, address).join();
        return createFutureProxy(address, taskName);
    }

    private void attachHazelcastInstance(HazelcastInstanceAware future) {
        future.setHazelcastInstance(getNodeEngine().getHazelcastInstance());
    }

}
