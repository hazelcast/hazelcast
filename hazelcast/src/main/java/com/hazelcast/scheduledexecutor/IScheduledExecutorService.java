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

package com.hazelcast.scheduledexecutor;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.DistributedObject;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Distributed &amp; durable implementation similar to, but not directly
 * inherited {@link ScheduledExecutorService}.
 * <code>IScheduledExecutorService</code> provides similar API to the
 * <code>ScheduledExecutorService</code> with some exceptions but also
 * additional methods like scheduling tasks on a specific member, on a member
 * who is owner of a specific key, executing a tasks on multiple members etc.
 *
 * <p>Tasks (<code>Runnable</code> and/or <code>Callable</code>) scheduled on
 * any partition through an <code>IScheduledExecutorService</code>, yield
 * some durability characteristics.
 * <ul>
 * <li>When a node goes down (up to <code>durability</code> config), the
 * scheduled task will get re-scheduled on a replica node.
 * <li>In the event of a partition migration, the task will be re-scheduled
 * on the destination node.
 * </ul>
 *
 * <b>Note: </b> The above characteristics don't apply when scheduled on a
 * <code>Member</code>.
 *
 * <p>Upon scheduling, a task acquires a resource handler, see
 * {@link ScheduledTaskHandler}. The handler is generated before the actual
 * scheduling of the task on the node, which allows for a way to access the
 * future in an event of a node failure, immediately after scheduling, and
 * also guarantees no duplicates in the cluster by utilising a unique name
 * per task. A name can also be defined by the user by having the
 * <code>Runnable</code> or <code>Callable</code> implement the {@link NamedTask}.
 * Alternatively, one can wrap any task using the
 * {@link TaskUtils#named(String, Callable)} or
 * {@link TaskUtils#named(String, Runnable)} for simplicity.
 *
 * <p>One difference of this service in comparison to
 * {@link ScheduledExecutorService} is the
 * {@link #scheduleAtFixedRate(Runnable, long, long, TimeUnit)} which has
 * similar semantic to
 * {@link java.util.concurrent.ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}.
 * It guarantees a task won't be executed by multiple threads concurrently.
 * The difference is that this service will skip a scheduled execution if
 * another thread is still running the same task, instead of postponing its
 * execution.
 *
 * <p>The other difference is this service does not offer an equivalent of
 * {@link java.util.concurrent.ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)}
 *
 * <p>Tasks that are holding state that needs to be also durable across
 * partitions, will need to implement the {@link StatefulTask} interface.
 *
 * <p>Supports split brain protection {@link SplitBrainProtectionConfig} since 3.10
 * in cluster versions 3.10 and higher.
 */
public interface IScheduledExecutorService
        extends DistributedObject {

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay.
     *
     * @param command the task to execute
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @param <V>     the result type of the returned ScheduledFuture
     * @return a ScheduledFuture representing pending completion of the task and whose
     * {@code get()} method will return {@code null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    @Nonnull
    <V> IScheduledFuture<V> schedule(@Nonnull Runnable command,
                                     long delay, @Nonnull TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay.
     *
     * @param command the task to execute
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @param <V>     the return type of callable tasks
     * @return a ScheduledFuture representing pending completion of the task and whose
     * {@code get()} method will return {@code null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    @Nonnull
    <V> IScheduledFuture<V> schedule(@Nonnull Callable<V> command,
                                     long delay, @Nonnull TimeUnit unit);

    /**
     * Creates and executes a periodic action that becomes enabled first after
     * the given initial delay, and subsequently with the given period.
     * Executions will commence after {@code initialDelay} then
     * {@code initialDelay+period}, then {@code initialDelay + 2 * period},
     * and so on. If any execution of this task takes longer than its period,
     * then subsequent execution will be skipped. If any execution of the task
     * encounters an exception, subsequent executions are suppressed.
     * Otherwise, the task will only terminate via cancellation or termination
     * of the executor.
     *
     * @param command      the task to execute
     * @param initialDelay the time to delay first execution
     * @param period       the period between successive executions
     * @param unit         the time unit of the initialDelay and period parameters
     * @param <V>          the result type of the returned ScheduledFuture
     * @return a ScheduledFuture representing pending completion of the task, and whose
     * {@code get()} method will throw an exception upon cancellation
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    @Nonnull
    <V> IScheduledFuture<V> scheduleAtFixedRate(@Nonnull Runnable command,
                                                long initialDelay, long period, @Nonnull TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the
     * given delay at the given {@link Member}.
     *
     * @param command the task to execute
     * @param member  the member to execute the task
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @param <V>     the result type of the returned ScheduledFuture
     * @return a ScheduledFuture representing pending completion of the task and whose
     * {@code get()} method will return {@code null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    @Nonnull
    <V> IScheduledFuture<V> scheduleOnMember(@Nonnull Runnable command,
                                             @Nonnull Member member,
                                             long delay, @Nonnull TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the
     * given delay at the given {@link Member}.
     *
     * @param command the task to execute
     * @param member  the member to execute the task
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @param <V>     the return type of callable tasks
     * @return a ScheduledFuture representing pending completion of the task and whose
     * {@code get()} method will return {@code null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    @Nonnull
    <V> IScheduledFuture<V> scheduleOnMember(@Nonnull Callable<V> command,
                                             @Nonnull Member member,
                                             long delay, @Nonnull TimeUnit unit);

    /**
     * Creates and executes a periodic action that becomes enabled first after
     * the given initial delay, and subsequently with the given period at the
     * given {@link Member}.
     * Executions will commence after {@code initialDelay} then
     * {@code initialDelay+period}, then {@code initialDelay + 2 * period}, and
     * so on. If any execution of this task takes longer than its period, then
     * subsequent execution will be skipped. If any execution of the task
     * encounters an exception, subsequent executions are suppressed.
     * Otherwise, the task will only terminate via cancellation or termination
     * of the executor.
     *
     * @param command      the task to execute
     * @param member       the member to execute the task
     * @param initialDelay the time to delay first execution
     * @param period       the period between successive executions
     * @param unit         the time unit of the initialDelay and period parameters
     * @param <V>          the result type of the returned ScheduledFuture
     * @return a ScheduledFuture representing pending completion of the task, and whose
     * {@code get()} method will throw an exception upon cancellation
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    @Nonnull
    <V> IScheduledFuture<V> scheduleOnMemberAtFixedRate(@Nonnull Runnable command,
                                                        @Nonnull Member member,
                                                        long initialDelay, long period, @Nonnull TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the
     * given delay on the partition owner of the given key.
     *
     * @param command the task to execute
     * @param key     the key to identify the partition owner, which will execute the task
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @param <V>     the result type of the returned ScheduledFuture
     * @return a ScheduledFuture representing pending completion of the task and whose
     * {@code get()} method will return {@code null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    @Nonnull
    <V> IScheduledFuture<V> scheduleOnKeyOwner(@Nonnull Runnable command,
                                               @Nonnull Object key,
                                               long delay, @Nonnull TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the
     * given delay on the partition owner of the given key.
     *
     * @param command the task to execute
     * @param key     the key to identify the partition owner, which will execute the task
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @param <V>     the return type of callable tasks
     * @return a ScheduledFuture representing pending completion of the task and whose
     * {@code get()} method will return {@code null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    @Nonnull
    <V> IScheduledFuture<V> scheduleOnKeyOwner(@Nonnull Callable<V> command,
                                               @Nonnull Object key,
                                               long delay, @Nonnull TimeUnit unit);

    /**
     * Creates and executes a periodic action that becomes enabled first after
     * the given initial delay, and subsequently with the given period on the
     * partition owner of the given key. Executions will commence after
     * {@code initialDelay} then {@code initialDelay+period}, then
     * {@code initialDelay + 2 * period}, and so on. If any execution of this
     * task takes longer than its period, then subsequent execution will be skipped.
     * If any execution of the task encounters an exception, subsequent executions
     * are suppressed.
     * Otherwise, the task will only terminate via cancellation or termination
     * of the executor.
     *
     * @param command      the task to execute
     * @param key          the key to identify the partition owner, which will execute the task
     * @param initialDelay the time to delay first execution
     * @param period       the period between successive executions
     * @param unit         the time unit of the initialDelay and period parameters
     * @param <V>          the result type of the returned ScheduledFuture
     * @return a ScheduledFuture representing pending completion of the task, and whose
     * {@code get()} method will throw an exception upon cancellation
     * @throws RejectedExecutionException if the task cannot be  scheduled for execution
     * @throws NullPointerException       if command is null
     */
    @Nonnull
    <V> IScheduledFuture<V> scheduleOnKeyOwnerAtFixedRate(@Nonnull Runnable command,
                                                          @Nonnull Object key,
                                                          long initialDelay, long period, @Nonnull TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the
     * given delay on all cluster {@link Member}s.
     *
     * <p>
     * <b>Note:</b> In the event of Member leaving the cluster, for whatever
     * reason, the task is lost. If a new member is added, the task will not
     * get scheduled there automatically.
     *
     * @param command the task to execute
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @param <V>     the result type of the returned ScheduledFuture
     * @return a ScheduledFuture representing pending completion of the task and whose
     * {@code get()} method will return {@code null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    @Nonnull
    <V> Map<Member, IScheduledFuture<V>> scheduleOnAllMembers(@Nonnull Runnable command,
                                                              long delay, @Nonnull TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the
     * given delay on all cluster {@link Member}s.
     *
     * <p>
     * <b>Note:</b> In the event of Member leaving the cluster, for whatever
     * reason, the task is lost. If a new member is added, the task will not
     * get scheduled there automatically.
     *
     * @param command the task to execute
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @param <V>     the return type of callable tasks
     * @return a ScheduledFuture representing pending completion of the task and whose
     * {@code get()} method will return {@code null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    @Nonnull
    <V> Map<Member, IScheduledFuture<V>> scheduleOnAllMembers(@Nonnull Callable<V> command,
                                                              long delay, @Nonnull TimeUnit unit);

    /**
     * Creates and executes a periodic action that becomes enabled first after
     * the given initial delay, and subsequently with the given period on all
     * cluster {@link Member}s. Executions will commence after
     * {@code initialDelay} then {@code initialDelay+period}, then
     * {@code initialDelay + 2 * period}, and so on. If any execution of this
     * task takes longer than its period, then subsequent execution will be
     * skipped. If any execution of the task encounters an exception, subsequent
     * executions are suppressed.
     * Otherwise, the task will only terminate via cancellation or termination
     * of the executor.
     *
     * <p>
     * <b>Note: </b> In the event of Member leaving the cluster, for whatever
     * reason, the task is lost. If a new member is added, the task will not
     * get scheduled there automatically.
     *
     * @param command      the task to execute
     * @param initialDelay the time to delay first execution
     * @param period       the period between successive executions
     * @param unit         the time unit of the initialDelay and period parameters
     * @param <V>          the result type of the returned ScheduledFuture
     * @return a ScheduledFuture representing pending completion of the task, and whose
     * {@code get()} method will throw an exception upon cancellation
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    @Nonnull
    <V> Map<Member, IScheduledFuture<V>> scheduleOnAllMembersAtFixedRate(@Nonnull Runnable command,
                                                                         long initialDelay, long period, @Nonnull TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the
     * given delay on all {@link Member}s given.
     *
     * <p>
     * <b>Note: </b> In the event of Member leaving the cluster, for whatever
     * reason, the task is lost.
     *
     * @param command the task to execute
     * @param members the collections of members - where to execute the task
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @param <V>     the result type of the returned ScheduledFuture
     * @return a ScheduledFuture representing pending completion of the task and whose
     * {@code get()} method will return {@code null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    @Nonnull
    <V> Map<Member, IScheduledFuture<V>> scheduleOnMembers(@Nonnull Runnable command,
                                                           @Nonnull Collection<Member> members,
                                                           long delay, @Nonnull TimeUnit unit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the
     * given delay on all {@link Member}s given.
     *
     * <p>
     * <b>Note: </b> In the event of Member leaving the cluster, for whatever
     * reason, the task is lost.
     *
     * @param command the task to execute
     * @param members the collections of members - where to execute the task
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @param <V>     the return type of callable tasks
     * @return a ScheduledFuture representing pending completion of the task and whose
     * {@code get()} method will return {@code null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    @Nonnull
    <V> Map<Member, IScheduledFuture<V>> scheduleOnMembers(@Nonnull Callable<V> command,
                                                           @Nonnull Collection<Member> members,
                                                           long delay, @Nonnull TimeUnit unit);

    /**
     * Creates and executes a periodic action that becomes enabled first after
     * the given initial delay, and subsequently with the given period on all
     * {@link Member}s given. Executions will commence after {@code initialDelay}
     * then {@code initialDelay+period}, then {@code initialDelay + 2 * period},
     * and so on. If any execution of this task takes longer than its period,
     * then subsequent execution will be skipped. If any execution of the task
     * encounters an exception, subsequent executions are suppressed.
     * Otherwise, the task will only terminate via cancellation or termination
     * of the executor.
     *
     * <p>
     * <b>Note: </b> In the event of Member leaving the cluster, for whatever
     * reason, the task is lost.
     *
     * @param command      the task to execute
     * @param members      the collections of members - where to execute the task
     * @param initialDelay the time to delay first execution
     * @param period       the period between successive executions
     * @param unit         the time unit of the initialDelay and period parameters
     * @param <V>          the result type of the returned ScheduledFuture
     * @return a ScheduledFuture representing pending completion of the task, and whose
     * {@code get()} method will throw an exception upon cancellation
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException       if command is null
     */
    @Nonnull
    <V> Map<Member, IScheduledFuture<V>> scheduleOnMembersAtFixedRate(@Nonnull Runnable command,
                                                                      @Nonnull Collection<Member> members,
                                                                      long initialDelay,
                                                                      long period, @Nonnull TimeUnit unit);

    /**
     * Creates a new {@link IScheduledFuture} from the given handler. This is
     * useful in case your member node or client from which the original
     * scheduling happened, went down, and now you want to access the
     * <code>ScheduledFuture</code> again.
     *
     * @param handler The handler of the task as found from {@link IScheduledFuture#getHandler()}
     * @param <V>     the result type of the returned ScheduledFuture
     * @return A new {@link IScheduledFuture} from the given handler.
     */
    @Nonnull
    <V> IScheduledFuture<V> getScheduledFuture(@Nonnull ScheduledTaskHandler handler);

    /**
     * Fetches and returns all scheduled (not disposed yet) futures from all
     * members in the cluster. If a member has no running tasks for this
     * scheduler, it wont be included in the returned {@link Map}.
     *
     * @param <V>     the result type of the returned ScheduledFuture
     * @return A {@link Map} with {@link Member} keys and a List of {@link IScheduledFuture}
     * found for this scheduler.
     */
    @Nonnull
    <V> Map<Member, List<IScheduledFuture<V>>> getAllScheduledFutures();

    /**
     * Initiates an orderly shutdown in which previously submitted tasks are
     * executed, but no new tasks will be accepted.
     * Invocation has no additional effect if already shut down.
     *
     * <p>This method does not wait for previously submitted tasks to complete
     * execution.
     */
    void shutdown();

}
