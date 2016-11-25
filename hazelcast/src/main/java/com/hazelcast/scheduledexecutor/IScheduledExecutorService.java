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

package com.hazelcast.scheduledexecutor;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Member;
import com.hazelcast.spi.annotation.Beta;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@Beta
public interface IScheduledExecutorService extends DistributedObject {

    IScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);

    <V> IScheduledFuture<V> schedule(Callable<V> command, long delay, TimeUnit unit);

    IScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay,
                                               long period, TimeUnit unit);

    IScheduledFuture<?> scheduleOnMember(Runnable command, Member member, long delay, TimeUnit unit);

    <V> IScheduledFuture<V> scheduleOnMember(Callable<V> command, Member member, long delay, TimeUnit unit);

    IScheduledFuture<?> scheduleOnMemberWithRepetition(Runnable command, Member member, long initialDelay, long period, TimeUnit unit);

    IScheduledFuture<?> scheduleOnKeyOwner(Runnable command, Object Key, long delay, TimeUnit unit);

    <V> IScheduledFuture<V> scheduleOnKeyOwner(Callable<V> command, Object Key, long delay, TimeUnit unit);

    IScheduledFuture<?> scheduleOnKeyOwnerWithRepetition(Runnable command, Object key, long initialDelay, long period, TimeUnit unit);

    Map<Member, IScheduledFuture<?>> scheduleOnAllMembers(Runnable command, long delay, TimeUnit unit);

    <V> Map<Member, IScheduledFuture<V>> scheduleOnAllMembers(Callable<V> command, long delay, TimeUnit unit);

    Map<Member, IScheduledFuture<?>> scheduleOnAllMembersWithRepetition(Runnable command, long initialDelay, long period, TimeUnit unit);

    Map<Member, IScheduledFuture<?>> scheduleOnMembers(Runnable command, Collection<Member> members, long delay, TimeUnit unit);

    <V> Map<Member, IScheduledFuture<V>> scheduleOnMembers(Callable<V> command, Collection<Member> members, long delay, TimeUnit unit);

    Map<Member, IScheduledFuture<?>> scheduleOnMembersWithRepetition(Runnable command, Collection<Member> members, long initialDelay, long period, TimeUnit unit);

    <V> IScheduledFuture<V> getScheduled(ScheduledTaskHandler handler);

    Map<Member, List<IScheduledFuture<?>>> getAllScheduled();

    void shutdown();

}