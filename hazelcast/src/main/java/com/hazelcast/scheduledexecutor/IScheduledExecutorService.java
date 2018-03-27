/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Distributed & durable implementation similar to, but not directly inherited {@link ScheduledExecutorService}.
 * <code>IScheduledExecutorService</code> provides similar API to the <code>ScheduledExecutorService</code> with some exceptions
 * but also additional methods like scheduling tasks on a specific member, on a member who is owner of a specific key, executing a
 * tasks on multiple members etc.
 *
 * <p>Tasks (<tt>Runnable</tt> and/or <tt>Callable</tt>) scheduled on any partition through an <tt>IScheduledExecutorService</tt>,
 * yield some durability characteristics.
 * <ul>
 *     <li>When a node goes down (up to <tt>durability</tt> config), the scheduled task will get re-scheduled on a replica node.
 *     <li>In the event of a partition migration, the task will be re-scheduled on the destination node.
 * </ul>
 *
 * <b>Note: </b> The above characteristics don't apply when scheduled on a <tt>Member</tt>.
 *
 * <p>Upon scheduling, a task acquires a resource handler, see {@link ScheduledTaskHandler}. The handler is generated before the
 * actual scheduling of the task on the node, which allows for a way to access the future in an event of a node failure,
 * immediately after scheduling, and also guarantees no duplicates in the cluster by utilising a unique name per task. A name can
 * also be defined by the user by having the <tt>Runnable</tt> or <tt>Callable</tt> implement the {@link NamedTask}.
 * Alternatively, one can wrap any task using the {@link TaskUtils#named(String, Callable)}
 * or {@link TaskUtils#named(String, Runnable)} for simplicity.
 *
 * <p>One difference of this service in comparison to {@link ScheduledExecutorService} is the {@link
 * #scheduleAtFixedRate(Runnable, long, long, TimeUnit)} which has similar semantic to {@link
 * java.util.concurrent.ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}. It guarantees a task won't
 * be executed by multiple threads concurrently. The difference is that this service will skip a scheduled execution if another
 * thread is still running the same task, instead of postponing its execution.
 *
 * <p>The other difference is this service does not offer an equivalent of
 * {@link java.util.concurrent.ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)}
 *
 * <p>Tasks that are holding state that needs to be also durable across partitions, will need to implement the {@link
 * StatefulTask} interface.
 *
 * <p>Supports Quorum {@link com.hazelcast.config.QuorumConfig} since 3.10 in cluster versions 3.10 and higher.
 */
public interface IScheduledExecutorService
        extends DistributedObject {

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay.
     *
     * @param command the task to execute
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose {@code get()} method will return {@code
     * null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    IScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay.
     *
     * @param command the task to execute
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose {@code get()} method will return {@code
     * null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    <V> IScheduledFuture<V> schedule(Callable<V> command, long delay, TimeUnit unit);

    /**
     * Creates and executes a periodic action that becomes enabled first after the given initial delay, and subsequently with the
     * given period. Executions will commence after {@code initialDelay} then {@code initialDelay+period}, then {@code
     * initialDelay + 2 * period}, and so on. If any execution of this task takes longer than its period, then subsequent
     * execution will be skipped.
     *
     * @param command      the task to execute
     * @param initialDelay the time to delay first execution
     * @param period       the period between successive executions
     * @param unit         the time unit of the initialDelay and period parameters
     * @return a ScheduledFuture representing pending completion of the task, and whose {@code get()} method will throw an
     * exception upon cancellation
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    IScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay at the given {@link Member}.
     *
     * @param command the task to execute
     * @param member  the member to execute the task
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose {@code get()} method will return {@code
     * null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    IScheduledFuture<?> scheduleOnMember(Runnable command, Member member, long delay, TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay at the given {@link Member}.
     *
     * @param command the task to execute
     * @param member  the member to execute the task
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose {@code get()} method will return {@code
     * null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    <V> IScheduledFuture<V> scheduleOnMember(Callable<V> command, Member member, long delay, TimeUnit unit);

    /**
     * Creates and executes a periodic action that becomes enabled first after the given initial delay, and subsequently with the
     * given period at the given {@link Member}. Executions will commence after {@code initialDelay} then {@code
     * initialDelay+period}, then {@code initialDelay + 2 * period}, and so on. If any execution of this task takes longer than
     * its period, then subsequent execution will be skipped.
     *
     * @param command      the task to execute
     * @param member       the member to execute the task
     * @param initialDelay the time to delay first execution
     * @param period       the period between successive executions
     * @param unit         the time unit of the initialDelay and period parameters
     * @return a ScheduledFuture representing pending completion of the task, and whose {@code get()} method will throw an
     * exception upon cancellation
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    IScheduledFuture<?> scheduleOnMemberAtFixedRate(Runnable command, Member member, long initialDelay, long period,
                                                    TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay on the partition owner of the given key.
     *
     * @param command the task to execute
     * @param key     the key to identify the partition owner, which will execute the task
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose {@code get()} method will return {@code
     * null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    IScheduledFuture<?> scheduleOnKeyOwner(Runnable command, Object key, long delay, TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay on the partition owner of the given key.
     *
     * @param command the task to execute
     * @param key     the key to identify the partition owner, which will execute the task
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose {@code get()} method will return {@code
     * null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    <V> IScheduledFuture<V> scheduleOnKeyOwner(Callable<V> command, Object key, long delay, TimeUnit unit);

    /**
     * Creates and executes a periodic action that becomes enabled first after the given initial delay, and subsequently with the
     * given period on the partition owner of the given key. Executions will commence after {@code initialDelay} then {@code
     * initialDelay+period}, then {@code initialDelay + 2 * period}, and so on. If any execution of this task takes longer than
     * its period, then subsequent execution will be skipped.
     *
     * @param command      the task to execute
     * @param key          the key to identify the partition owner, which will execute the task
     * @param initialDelay the time to delay first execution
     * @param period       the period between successive executions
     * @param unit         the time unit of the initialDelay and period parameters
     * @return a ScheduledFuture representing pending completion of the task, and whose {@code get()} method will throw an
     * exception upon cancellation
     * @throws RejectedExecutionException if the task cannot be  scheduled for execution
     * @throws NullPointerException       if command is null
     */
    IScheduledFuture<?> scheduleOnKeyOwnerAtFixedRate(Runnable command, Object key, long initialDelay, long period,
                                                      TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay on all cluster {@link Member}s.
     *
     * <p>
     * <b>Note:</b> In the event of Member leaving the cluster, for whatever reason, the task is lost. If a new member is
     * added, the task will not get scheduled there automatically.
     *
     * @param command the task to execute
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose {@code get()} method will return {@code
     * null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    Map<Member, IScheduledFuture<?>> scheduleOnAllMembers(Runnable command, long delay, TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay on all cluster {@link Member}s.
     *
     * <p>
     * <b>Note:</b> In the event of Member leaving the cluster, for whatever reason, the task is lost. If a new member is
     * added, the task will not get scheduled there automatically.
     *
     * @param command the task to execute
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose {@code get()} method will return {@code
     * null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    <V> Map<Member, IScheduledFuture<V>> scheduleOnAllMembers(Callable<V> command, long delay, TimeUnit unit);

    /**
     * Creates and executes a periodic action that becomes enabled first after the given initial delay, and subsequently with the
     * given period on all cluster {@link Member}s. Executions will commence after {@code initialDelay} then {@code
     * initialDelay+period}, then {@code initialDelay + 2 * period}, and so on. If any execution of this task takes longer than
     * its period, then subsequent execution will be skipped.
     *
     * <p>
     * <b>Note: </b> In the event of Member leaving the cluster, for whatever reason, the task is lost. If a new member is
     * added, the task will not get scheduled there automatically.
     *
     * @param command      the task to execute
     * @param initialDelay the time to delay first execution
     * @param period       the period between successive executions
     * @param unit         the time unit of the initialDelay and period parameters
     * @return a ScheduledFuture representing pending completion of the task, and whose {@code get()} method will throw an
     * exception upon cancellation
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    Map<Member, IScheduledFuture<?>> scheduleOnAllMembersAtFixedRate(Runnable command, long initialDelay, long period,
                                                                     TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay on all {@link Member}s given.
     *
     * <p>
     * <b>Note: </b> In the event of Member leaving the cluster, for whatever reason, the task is lost.
     *
     * @param command the task to execute
     * @param members the collections of members - where to execute the task
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose {@code get()} method will return {@code
     * null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    Map<Member, IScheduledFuture<?>> scheduleOnMembers(Runnable command, Collection<Member> members, long delay, TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay on all {@link Member}s given.
     *
     * <p>
     * <b>Note: </b> In the event of Member leaving the cluster, for whatever reason, the task is lost.
     *
     * @param command the task to execute
     * @param members the collections of members - where to execute the task
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose {@code get()} method will return {@code
     * null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    <V> Map<Member, IScheduledFuture<V>> scheduleOnMembers(Callable<V> command, Collection<Member> members, long delay,
                                                           TimeUnit unit);

    /**
     * Creates and executes a periodic action that becomes enabled first after the given initial delay, and subsequently with the
     * given period on all {@link Member}s given. Executions will commence after {@code initialDelay} then {@code
     * initialDelay+period}, then {@code initialDelay + 2 * period}, and so on. If any execution of this task takes longer than
     * its period, then subsequent execution will be skipped.
     *
     * <p>
     * <b>Note: </b> In the event of Member leaving the cluster, for whatever reason, the task is lost.
     *
     * @param command      the task to execute
     * @param members      the collections of members - where to execute the task
     * @param initialDelay the time to delay first execution
     * @param period       the period between successive executions
     * @param unit         the time unit of the initialDelay and period parameters
     * @return a ScheduledFuture representing pending completion of the task, and whose {@code get()} method will throw an
     * exception upon cancellation
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    Map<Member, IScheduledFuture<?>> scheduleOnMembersAtFixedRate(Runnable command, Collection<Member> members, long initialDelay,
                                                                  long period, TimeUnit unit);

    /**
     * Creates a new {@link IScheduledFuture} from the given handler. This is useful in case your member node or client from which
     * the original scheduling happened, went down, and now you want to access the <tt>ScheduledFuture</tt> again.
     *
     * @param handler The handler of the task as found from {@link IScheduledFuture#getHandler()}
     * @param <V>     The return type of callable tasks
     * @return A new {@link IScheduledFuture} from the given handler.
     */
    <V> IScheduledFuture<V> getScheduledFuture(ScheduledTaskHandler handler);

    /**
     * Fetches and returns all scheduled (not disposed yet) futures from all members in the cluster. If a member has no running
     * tasks for this scheduler, it wont be included in the returned {@link Map}.
     *
     * @return A {@link Map} with {@link Member} keys and a List of {@link IScheduledFuture} found for this scheduler.
     */
    <V> Map<Member, List<IScheduledFuture<V>>> getAllScheduledFutures();

    /**
     * Initiates an orderly shutdown in which previously submitted tasks are executed, but no new tasks will be accepted.
     * Invocation has no additional effect if already shut down.
     *
     * <p>This method does not wait for previously submitted tasks to complete execution.
     */
    void shutdown();

}
