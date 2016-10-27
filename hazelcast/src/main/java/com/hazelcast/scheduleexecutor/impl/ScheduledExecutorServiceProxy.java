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
import com.hazelcast.core.Member;
import com.hazelcast.scheduleexecutor.IScheduledExecutorService;
import com.hazelcast.scheduleexecutor.IScheduledFuture;
import com.hazelcast.scheduleexecutor.IdentifiedRunnable;
import com.hazelcast.scheduleexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduleexecutor.impl.operations.ScheduleTaskOperation;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.UuidUtil;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ScheduledExecutorServiceProxy
        extends AbstractDistributedObject<DistributedScheduledExecutorService>
        implements IScheduledExecutorService {

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

    @Override
    public IScheduledFuture schedule(String name, Runnable command, long delay, TimeUnit unit) {
        RunnableDefinition definition = new RunnableDefinition(
                RunnableDefinition.Type.SINGLE_RUN, name, command, delay, unit);

        return invokeAsync(name, new ScheduleTaskOperation(getName(), definition));
    }

    @Override
    public IScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduleAtFixedRate(UuidUtil.newUnsecureUuidString(), command, initialDelay, period, unit);
    }

    @Override
    public IScheduledFuture<?> scheduleAtFixedRate(String name, Runnable command, long initialDelay, long period, TimeUnit unit) {
        RunnableDefinition definition = new RunnableDefinition(
                RunnableDefinition.Type.PERIODIC, name, command, initialDelay, 0, period, unit);

        return invokeAsync(name, new ScheduleTaskOperation(getName(), definition));
    }

    @Override
    public IScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return scheduleWithFixedDelay(UuidUtil.newUnsecureUuidString(), command, initialDelay, delay, unit);
    }

    @Override
    public IScheduledFuture<?> scheduleWithFixedDelay(String name, Runnable command, long initialDelay, long delay, TimeUnit unit) {
        RunnableDefinition definition = new RunnableDefinition(
                RunnableDefinition.Type.FIXED_DELAY, name, command, initialDelay, delay, unit);

        return invokeAsync(name, new ScheduleTaskOperation(getName(), definition));
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
    public IScheduledFuture<?> scheduleOnMemberAtFixedRate(Runnable command, Member member, long initialDelay, long period,
                                                           TimeUnit unit) {
        return scheduleOnMemberAtFixedRate(UuidUtil.newUnsecureUuidString(), command, member, initialDelay, period, unit);
    }

    @Override
    public IScheduledFuture<?> scheduleOnMemberAtFixedRate(String name, Runnable command, Member member, long initialDelay,
                                                           long period, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IScheduledFuture<?> scheduleOnMemberWithFixedDelay(Runnable command, Member member, long initialDelay, long delay,
                                                              TimeUnit unit) {
        return scheduleOnMemberWithFixedDelay(UuidUtil.newUnsecureUuidString(), command, member, initialDelay, delay, unit);
    }

    @Override
    public IScheduledFuture<?> scheduleOnMemberWithFixedDelay(String name, Runnable command, Member member, long initialDelay,
                                                              long delay, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IScheduledFuture<?> scheduleOnKeyOwner(Runnable command, Object key, long delay, TimeUnit unit) {
        return scheduleOnKeyOwner(UuidUtil.newUnsecureUuidString(), command, key, delay, unit);
    }

    @Override
    public IScheduledFuture<?> scheduleOnKeyOwner(String name, Runnable command, Object key, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IScheduledFuture<?> scheduleOnKeyOwnerAtFixedRate(Runnable command, Object key, long initialDelay, long period,
                                                             TimeUnit unit) {
        return scheduleOnKeyOwnerAtFixedRate(UuidUtil.newUnsecureUuidString(), command, key, initialDelay, period, unit);
    }

    @Override
    public IScheduledFuture<?> scheduleOnKeyOwnerAtFixedRate(String name, Runnable command, Object key, long initialDelay,
                                                             long period, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IScheduledFuture<?> scheduleOnKeyOwnerWithFixedDelay(Runnable command, Object key, long initialDelay, long delay,
                                                                TimeUnit unit) {
        return scheduleOnKeyOwnerWithFixedDelay(UuidUtil.newUnsecureUuidString(), command, key, initialDelay, delay, unit);
    }

    @Override
    public IScheduledFuture<?> scheduleOnKeyOwnerWithFixedDelay(String name, Runnable command, Object key, long initialDelay,
                                                                long delay, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnAllMembers(Runnable command, long delay, TimeUnit unit) {
        return scheduleOnAllMembers(UuidUtil.newUnsecureUuidString(), command, delay, unit);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnAllMembers(String name, Runnable command, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnAllMembersAtFixedRate(Runnable command, long initialDelay, long period,
                                                                           TimeUnit unit) {
        return scheduleOnAllMembersAtFixedRate(UuidUtil.newUnsecureUuidString(), command, initialDelay, period, unit);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnAllMembersAtFixedRate(String name, Runnable command, long initialDelay, long period,
                                                               TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnAllMembersWithFixedDelay(Runnable command, long initialDelay, long delay,
                                                                              TimeUnit unit) {
        return scheduleOnAllMembersWithFixedDelay(UuidUtil.newUnsecureUuidString(), command, initialDelay, delay, unit);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnAllMembersWithFixedDelay(String name, Runnable command, long initialDelay, long delay,
                                                                  TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnMembers(Runnable command, Collection<Member> members, long delay,
                                                             TimeUnit unit) {
        return scheduleOnMembers(UuidUtil.newUnsecureUuidString(), command, members, delay, unit);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnMembers(String name, Runnable command, Collection<Member> members, long delay,
                                                 TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnMembersAtFixedRate(Runnable command, Collection<Member> members,
                                                                        long initialDelay, long period, TimeUnit unit) {
        return scheduleOnMembersAtFixedRate(UuidUtil.newUnsecureUuidString(), command, members, initialDelay, period, unit);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnMembersAtFixedRate(String name, Runnable command, Collection<Member> members,
                                                            long initialDelay, long period, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnMembersWithFixedDelay(Runnable command, Collection<Member> members,
                                                                           long initialDelay, long delay, TimeUnit unit) {
        return scheduleOnMembersWithFixedDelay(UuidUtil.newUnsecureUuidString(), command, members, initialDelay, delay, unit);
    }

    @Override
    public Map<Member, IScheduledFuture<?>> scheduleOnMembersWithFixedDelay(String name, Runnable command, Collection<Member> members,
                                                               long initialDelay, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IScheduledFuture<?> getScheduled(ScheduledTaskHandler handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Member, IScheduledFuture<?>> getAllScheduled() {
        return null;
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

    private IScheduledFuture<?> invokeAsync(String taskName, Operation op) {
        int partitionId = partitionRandom.nextInt(partitionCount);

        op.setPartitionId(partitionId);
        invokeOnPartition(op);
        return attachHazelcastInstanceIfNeeded(
                new ScheduledFutureProxy(
                        ScheduledTaskHandler.of(partitionId, getName(), taskName)));
    }

    private <V> V attachHazelcastInstanceIfNeeded(V response) {
        if (response instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) response).setHazelcastInstance(getNodeEngine().getHazelcastInstance());
        }

        return response;
    }
}
