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

package com.hazelcast.scheduleexecutor;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.spi.annotation.Beta;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Beta
public interface IScheduledExecutorService extends DistributedObject {

    IScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);

    IScheduledFuture<?> schedule(String name, Runnable command, long delay, TimeUnit unit);

    IScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay,
                                               long period, TimeUnit unit);

    IScheduledFuture<?> scheduleWithRepetition(String name, Runnable command, long initialDelay,
                                               long period, TimeUnit unit);

    IScheduledFuture<?> scheduleOnMember(Runnable command, Member member, long delay, TimeUnit unit);

    IScheduledFuture<?> scheduleOnMember(String name, Runnable command, Member member, long delay, TimeUnit unit);

    IScheduledFuture<?> scheduleOnMemberWithRepetition(Runnable command, Member member, long initialDelay, long period, TimeUnit unit);

    IScheduledFuture<?> scheduleOnMemberWithRepetition(String name, Runnable command, Member member, long initialDelay, long period, TimeUnit unit);

    IScheduledFuture<?> scheduleOnKeyOwner(Runnable command, Object Key,long delay, TimeUnit unit);

    IScheduledFuture<?> scheduleOnKeyOwner(String name, Runnable command, Object Key,long delay, TimeUnit unit);

    IScheduledFuture<?> scheduleOnKeyOwnerWithRepetition(Runnable command, Object key, long initialDelay, long period, TimeUnit unit);

    IScheduledFuture<?> scheduleOnKeyOwnerWithRepetition(String name, Runnable command, Object key, long initialDelay, long period, TimeUnit unit);

    Map<Member, IScheduledFuture<?>> scheduleOnAllMembers(Runnable command, long delay, TimeUnit unit);

    Map<Member, IScheduledFuture<?>> scheduleOnAllMembers(String name, Runnable command, long delay, TimeUnit unit);

    Map<Member, IScheduledFuture<?>> scheduleOnAllMembersWithRepetition(Runnable command, long initialDelay, long period, TimeUnit unit);

    Map<Member, IScheduledFuture<?>> scheduleOnAllMembersWithRepetition(String name, Runnable command, long initialDelay, long period, TimeUnit unit);

    Map<Member, IScheduledFuture<?>> scheduleOnMembers(Runnable command, Collection<Member> members, long delay, TimeUnit unit);

    Map<Member, IScheduledFuture<?>> scheduleOnMembers(String name, Runnable command, Collection<Member> members, long delay, TimeUnit unit);

    Map<Member, IScheduledFuture<?>> scheduleOnMembersWithRepetition(Runnable command, Collection<Member> members, long initialDelay, long period, TimeUnit unit);

    Map<Member, IScheduledFuture<?>> scheduleOnMembersWithRepetition(String name, Runnable command, Collection<Member> members, long initialDelay, long period, TimeUnit unit);

    IScheduledFuture<?> getScheduled(ScheduledTaskHandler handler);

    ICompletableFuture<Map<Member, IScheduledFuture<?>>> getAllScheduled();

    void shutdown();

    List<IdentifiedRunnable> shutdownNow();

    boolean isShutdown();

    boolean isTerminated();

    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;

}