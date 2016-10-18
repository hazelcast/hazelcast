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

package com.hazelcast.scheduleexecutor.impl;

import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.mapreduce.impl.HashMapAdapter;
import com.hazelcast.nio.Address;
import com.hazelcast.scheduleexecutor.IScheduledExecutorService;
import com.hazelcast.scheduleexecutor.IScheduledFuture;
import com.hazelcast.scheduleexecutor.IdentifiedRunnable;
import com.hazelcast.scheduleexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduleexecutor.impl.operations.GetAllScheduledOperation;
import com.hazelcast.scheduleexecutor.impl.operations.ScheduleTaskOperation;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static com.hazelcast.util.FutureUtil.returnWithDeadline;

public class ScheduledExecutorServiceProxy
        extends AbstractDistributedObject<DistributedScheduledExecutorService>
        implements IScheduledExecutorService {

    public static final int GET_ALL_SCHEDULED_TIMEOUT = 10;

    private final String name;

    private final int partitionCount;

    private final Random partitionRandom = new Random();

    public ScheduledExecutorServiceProxy(String name, NodeEngine nodeEngine,
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
        RunnableDefinition definition = new RunnableDefinition(
                RunnableDefinition.Type.SINGLE_RUN, name, command, delay, unit);

        return submitAsync(name, new ScheduleTaskOperation(getName(), definition));
    }

    @Override
    public IScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduleWithRepetition(UuidUtil.newUnsecureUuidString(), command, initialDelay, period, unit);
    }

    @Override
    public IScheduledFuture<?> scheduleWithRepetition(String name, Runnable command, long initialDelay, long period, TimeUnit unit) {
        RunnableDefinition definition = new RunnableDefinition(
                RunnableDefinition.Type.WITH_REPETITION, name, command, initialDelay, 0, period, unit);

        return submitAsync(name, new ScheduleTaskOperation(getName(), definition));
    }

    @Override
    public IScheduledFuture<?> scheduleOnMember(Runnable command, Member member, long delay, TimeUnit unit) {
        return scheduleOnMember(UuidUtil.newUnsecureUuidString(), command, member, delay, unit);
    }

    @Override
    public IScheduledFuture<?> scheduleOnMember(String name, Runnable command, Member member, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IScheduledFuture<?> scheduleOnMemberWithRepetition(Runnable command, Member member, long initialDelay, long period,
                                                              TimeUnit unit) {
        return scheduleOnMemberWithRepetition(UuidUtil.newUnsecureUuidString(), command, member, initialDelay, period, unit);
    }

    @Override
    public IScheduledFuture<?> scheduleOnMemberWithRepetition(String name, Runnable command, Member member, long initialDelay,
                                                              long period, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IScheduledFuture<?> scheduleOnKeyOwner(Runnable command, Object key, long delay, TimeUnit unit) {
        return scheduleOnKeyOwner(UuidUtil.newUnsecureUuidString(), command, key, delay, unit);
    }

    @Override
    public IScheduledFuture<?> scheduleOnKeyOwner(String name, Runnable command, Object key, long delay, TimeUnit unit) {
        int partitionId = getKeyPartitionId(key);

        RunnableDefinition definition = new RunnableDefinition(
                RunnableDefinition.Type.SINGLE_RUN, name, command, delay, unit);

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
        RunnableDefinition definition = new RunnableDefinition(
                RunnableDefinition.Type.WITH_REPETITION, name, command, initialDelay, 0, period, unit);

        return submitOnPartitionAsync(name, new ScheduleTaskOperation(getName(), definition), partitionId);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnAllMembers(Runnable command, long delay, TimeUnit unit) {
        return scheduleOnAllMembers(UuidUtil.newUnsecureUuidString(), command, delay, unit);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnAllMembers(String name, Runnable command, long delay, TimeUnit unit) {
        Map<Member, IScheduledFuture<?>> futures = new HashMapAdapter<Member, IScheduledFuture<?>>();
        for (Member member : getNodeEngine().getClusterService().getMembers()) {
            RunnableDefinition definition = new RunnableDefinition(
                    RunnableDefinition.Type.SINGLE_RUN, name, command, delay, unit);

            futures.put(member, submitOnMemberAsync(name, new ScheduleTaskOperation(getName(), definition), member));
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
        Map<Member, IScheduledFuture<?>> futures = new HashMapAdapter<Member, IScheduledFuture<?>>();
        for (Member member : getNodeEngine().getClusterService().getMembers()) {
            RunnableDefinition definition = new RunnableDefinition(
                    RunnableDefinition.Type.WITH_REPETITION, name, command, initialDelay, 0, period, unit);

            futures.put(member, submitOnMemberAsync(name, new ScheduleTaskOperation(getName(), definition), member));
        }

        return futures;
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnMembers(Runnable command, Collection<Member> members, long delay,
                                                             TimeUnit unit) {
        return scheduleOnMembers(UuidUtil.newUnsecureUuidString(), command, members, delay, unit);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnMembers(String name, Runnable command, Collection<Member> members, long delay,
                                                 TimeUnit unit) {
        Map<Member, IScheduledFuture<?>> futures = new HashMapAdapter<Member, IScheduledFuture<?>>();
        for (Member member : members) {
            RunnableDefinition definition = new RunnableDefinition(
                    RunnableDefinition.Type.SINGLE_RUN, name, command, delay, unit);

            futures.put(member, submitOnMemberAsync(name, new ScheduleTaskOperation(getName(), definition), member));
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
        Map<Member, IScheduledFuture<?>> futures = new HashMapAdapter<Member, IScheduledFuture<?>>();
        for (Member member : members) {
            RunnableDefinition definition = new RunnableDefinition(
                    RunnableDefinition.Type.WITH_REPETITION, name, command, initialDelay, 0, period, unit);

            futures.put(member, submitOnMemberAsync(name, new ScheduleTaskOperation(getName(), definition), member));
        }

        return futures;
    }

    @Override
    public IScheduledFuture<?> getScheduled(ScheduledTaskHandler handler) {
        return attachHazelcastInstanceIfNeeded(new ScheduledFutureProxy(handler));
    }

    @Override
    public ICompletableFuture<Map<Member, IScheduledFuture<?>>> getAllScheduled() {
        //TODO tkountis - Consider extra flavour with Executor provided as arg
        ExecutionService service = getNodeEngine().getExecutionService();
        ManagedExecutorService executor = getNodeEngine().getExecutionService().getExecutor(ASYNC_EXECUTOR);

        //TODO tkountis - Consider extra flavour with timeout as arg
        final long timeout = GET_ALL_SCHEDULED_TIMEOUT;

        return service.asCompletableFuture(executor.submit(new Callable<Map<Member, IScheduledFuture<?>>>() {
            @Override
            public Map<Member, IScheduledFuture<?>> call()
                    throws Exception {

                Map<Member, IScheduledFuture<?>> tasks =
                        new LinkedHashMap<Member, IScheduledFuture<?>>();

                List<Member> members = new ArrayList<Member>(getNodeEngine().getClusterService().getMembers());
                List<Future<ScheduledTaskHandler>> futures = new ArrayList<Future<ScheduledTaskHandler>>();
                for (Member member : members) {
                    Address address = member.getAddress();

                    ICompletableFuture<ScheduledTaskHandler> future =
                            getOperationService().invokeOnTarget(getServiceName(), new GetAllScheduledOperation(), address);

                    futures.add(future);
                }

                // TODO unit test - to make sure order returned is the expected one otherwise fail.
                List<ScheduledTaskHandler> resolvedFutures = new ArrayList<ScheduledTaskHandler>(
                        returnWithDeadline(futures, timeout, TimeUnit.SECONDS));

                for (int i = 0; i < members.size(); i++) {
                    Member member = members.get(i);
                    ScheduledTaskHandler handler = resolvedFutures.get(i);

                    tasks.put(member, attachHazelcastInstanceIfNeeded(
                            new ScheduledFutureProxy<ScheduledTaskHandler>(handler)));
                }

                return tasks;

            }
        }));
    }

    @Override
    public void shutdown() {

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

    private int getKeyPartitionId(Object key) {
        return getNodeEngine().getPartitionService().getPartitionId(key);
    }

    private IScheduledFuture<?> submitAsync(String taskName, Operation op) {
        int partitionId = partitionRandom.nextInt(partitionCount);
        return submitOnPartitionAsync(taskName, op, partitionId);
    }

    private IScheduledFuture<?> submitOnPartitionAsync(String taskName, Operation op, int partitionId) {
        op.setPartitionId(partitionId);
        invokeOnPartition(op);
        return attachHazelcastInstanceIfNeeded(
                new ScheduledFutureProxy(
                        ScheduledTaskHandler.of(partitionId, getName(), taskName)));
    }

    private IScheduledFuture<?> submitOnMemberAsync(String taskName, Operation op, Member member) {
        Address address = member.getAddress();

        getOperationService().invokeOnTarget(getServiceName(), op, address);
        return attachHazelcastInstanceIfNeeded(
                new ScheduledFutureProxy(
                        ScheduledTaskHandler.of(address, getName(), taskName)));
    }

    private <V> V attachHazelcastInstanceIfNeeded(V response) {
        if (response instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) response).setHazelcastInstance(getNodeEngine().getHazelcastInstance());
        }

        return response;
    }
}
