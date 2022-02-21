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

package com.hazelcast.core;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.executor.LocalExecutorStats;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Distributed implementation of {@link ExecutorService}.
 * IExecutorService provides additional methods like executing tasks
 * on a specific member, on a member who is owner of a specific key,
 * executing a tasks on multiple members and listening execution result using a callback.
 *
 * Supports split brain protection {@link SplitBrainProtectionConfig} since 3.10 in cluster
 * versions 3.10 and higher.
 *
 * @see ExecutorService
 * @see ExecutionCallback
 * @see MultiExecutionCallback
 */
public interface IExecutorService extends ExecutorService, DistributedObject {

    /**
     * Executes a task on a randomly selected member.
     *
     * @param command        the task that is executed on a randomly selected member
     * @param memberSelector memberSelector
     * @throws java.util.concurrent.RejectedExecutionException if no member is selected
     */
    void execute(@Nonnull Runnable command,
                 @Nonnull MemberSelector memberSelector);

    /**
     * Executes a task on the owner of the specified key.
     *
     * @param command a task executed on the owner of the specified key
     * @param key     the specified key
     */
    void executeOnKeyOwner(@Nonnull Runnable command,
                           @Nonnull Object key);

    /**
     * Executes a task on the specified member.
     *
     * @param command the task executed on the specified member
     * @param member  the specified member
     */
    void executeOnMember(@Nonnull Runnable command,
                         @Nonnull Member member);

    /**
     * Executes a task on each of the specified members.
     *
     * @param command the task executed on the specified members
     * @param members the specified members
     */
    void executeOnMembers(@Nonnull Runnable command,
                          @Nonnull Collection<Member> members);

    /**
     * Executes a task on each of the selected members.
     *
     * @param command        a task executed on each of the selected members
     * @param memberSelector memberSelector
     * @throws java.util.concurrent.RejectedExecutionException if no member is selected
     */
    void executeOnMembers(@Nonnull Runnable command,
                          @Nonnull MemberSelector memberSelector);

    /**
     * Executes a task on all of the known cluster members.
     *
     * @param command a task executed  on all of the known cluster members
     */
    void executeOnAllMembers(@Nonnull Runnable command);

    /**
     * Submits a task to a randomly selected member and returns a Future
     * representing that task.
     *
     * @param task           task submitted to a randomly selected member
     * @param memberSelector memberSelector
     * @param <T>            the result type of callable
     * @return a Future representing pending completion of the task
     * @throws java.util.concurrent.RejectedExecutionException if no member is selected
     */
    <T> Future<T> submit(@Nonnull Callable<T> task,
                         @Nonnull MemberSelector memberSelector);

    /**
     * Submits a task to the owner of the specified key and returns a Future
     * representing that task.
     *
     * @param task task submitted to the owner of the specified key
     * @param key  the specified key
     * @param <T>  the result type of callable
     * @return a Future representing pending completion of the task
     */
    <T> Future<T> submitToKeyOwner(@Nonnull Callable<T> task,
                                   @Nonnull Object key);

    /**
     * Submits a task to the specified member and returns a Future
     * representing that task.
     *
     * @param task   the task submitted to the specified member
     * @param member the specified member
     * @param <T>    the result type of callable
     * @return a Future representing pending completion of the task
     */
    <T> Future<T> submitToMember(@Nonnull Callable<T> task,
                                 @Nonnull Member member);

    /**
     * Submits a task to given members and returns
     * map of Member-Future pairs representing pending completion of the task on each member
     *
     * @param task    the task submitted to given members
     * @param members the given members
     * @param <T>     the result type of callable
     * @return map of Member-Future pairs representing pending completion of the task on each member
     */
    <T> Map<Member, Future<T>> submitToMembers(@Nonnull Callable<T> task,
                                               @Nonnull Collection<Member> members);

    /**
     * Submits a task to selected members and returns a
     * map of Member-Future pairs representing pending completion of the task on each member.
     *
     * @param task           the task submitted to selected members
     * @param memberSelector memberSelector
     * @param <T>            the result type of callable
     * @return map of Member-Future pairs representing pending completion of the task on each member
     * @throws java.util.concurrent.RejectedExecutionException if no member is selected
     */
    <T> Map<Member, Future<T>> submitToMembers(@Nonnull Callable<T> task,
                                               @Nonnull MemberSelector memberSelector);

    /**
     * Submits task to all cluster members and returns a
     * map of Member-Future pairs representing pending completion of the task on each member.
     *
     * @param task the task submitted to all cluster members
     * @param <T>  the result type of callable
     * @return map of Member-Future pairs representing pending completion of the task on each member
     */
    <T> Map<Member, Future<T>> submitToAllMembers(@Nonnull Callable<T> task);

    /**
     * Submits a task to a random member. Caller will be notified of the result of the task by
     * {@link ExecutionCallback#onResponse(Object)} or {@link ExecutionCallback#onFailure(Throwable)}.
     *
     * @param task     a task submitted to a random member
     * @param callback callback
     * @param <T>      the response type of callback
     */
    <T> void submit(@Nonnull Runnable task,
                    @Nullable ExecutionCallback<T> callback);

    /**
     * Submits a task to randomly selected members. Caller will be notified for the result of the task by
     * {@link ExecutionCallback#onResponse(Object)} or {@link ExecutionCallback#onFailure(Throwable)}.
     *
     * @param task           the task submitted to randomly selected members
     * @param memberSelector memberSelector
     * @param callback       callback
     * @param <T>            the response type of callback
     * @throws java.util.concurrent.RejectedExecutionException if no member is selected
     */
    <T> void submit(@Nonnull Runnable task,
                    @Nonnull MemberSelector memberSelector,
                    @Nullable ExecutionCallback<T> callback);

    /**
     * Submits a task to the owner of the specified key. Caller will be notified for the result of the task by
     * {@link ExecutionCallback#onResponse(Object)} or {@link ExecutionCallback#onFailure(Throwable)}.
     *
     * @param task     task submitted to the owner of the specified key
     * @param key      the specified key
     * @param callback callback
     * @param <T>      the response type of callback
     */
    <T> void submitToKeyOwner(@Nonnull Runnable task,
                              @Nonnull Object key,
                              @Nonnull ExecutionCallback<T> callback);

    /**
     * Submits a task to the specified member. Caller will be notified for the result of the task by
     * {@link ExecutionCallback#onResponse(Object)} or {@link ExecutionCallback#onFailure(Throwable)}.
     *
     * @param task     the task submitted to the specified member
     * @param member   the specified member
     * @param callback callback
     * @param <T>      the response type of callback
     */
    <T> void submitToMember(@Nonnull Runnable task,
                            @Nonnull Member member,
                            @Nullable ExecutionCallback<T> callback);

    /**
     * Submits a task to the specified members. Caller will be notified for the result of the each task by
     * {@link MultiExecutionCallback#onResponse(Member, Object)}, and when all tasks are completed,
     * {@link MultiExecutionCallback#onComplete(java.util.Map)} will be called.
     *
     * @param task     the task submitted to the specified members
     * @param members  the specified members
     * @param callback callback
     */
    void submitToMembers(@Nonnull Runnable task,
                         @Nonnull Collection<Member> members,
                         @Nonnull MultiExecutionCallback callback);

    /**
     * Submits task to the selected members. Caller will be notified for the result of the each task by
     * {@link MultiExecutionCallback#onResponse(Member, Object)}, and when all tasks are completed,
     * {@link MultiExecutionCallback#onComplete(java.util.Map)} will be called.
     *
     * @param task           the task submitted to the selected members
     * @param memberSelector memberSelector
     * @param callback       callback
     * @throws java.util.concurrent.RejectedExecutionException if no member is selected
     */
    void submitToMembers(@Nonnull Runnable task,
                         @Nonnull MemberSelector memberSelector,
                         @Nonnull MultiExecutionCallback callback);

    /**
     * Submits task to all the cluster members. Caller will be notified for the result of each task by
     * {@link MultiExecutionCallback#onResponse(Member, Object)}, and when all tasks are completed,
     * {@link MultiExecutionCallback#onComplete(java.util.Map)} will be called.
     *
     * @param task     the task submitted to all the cluster members
     * @param callback callback
     */
    void submitToAllMembers(@Nonnull Runnable task,
                            @Nonnull MultiExecutionCallback callback);

    /**
     * Submits a task to a random member. Caller will be notified for the result of the task by
     * {@link ExecutionCallback#onResponse(Object)} or {@link ExecutionCallback#onFailure(Throwable)}.
     *
     * @param task     the task submitted to a random member
     * @param callback callback
     * @param <T>      the result type of callable
     */
    <T> void submit(@Nonnull Callable<T> task,
                    @Nullable ExecutionCallback<T> callback);

    /**
     * Submits task to a randomly selected member. Caller will be notified for the result of the task by
     * {@link ExecutionCallback#onResponse(Object)} or {@link ExecutionCallback#onFailure(Throwable)}.
     *
     * @param task           the task submitted to a randomly selected member
     * @param memberSelector memberSelector
     * @param callback       callback
     * @param <T>            the result type of callable
     * @throws java.util.concurrent.RejectedExecutionException if no member is selected
     */
    <T> void submit(@Nonnull Callable<T> task,
                    @Nonnull MemberSelector memberSelector,
                    @Nullable ExecutionCallback<T> callback);

    /**
     * Submits task to the owner of the specified key. Caller will be notified for the result of the task by
     * {@link ExecutionCallback#onResponse(Object)} or {@link ExecutionCallback#onFailure(Throwable)}.
     *
     * @param task     the task submitted to the owner of the specified key
     * @param key      the specified key
     * @param callback callback
     * @param <T>      the result type of callable
     */
    <T> void submitToKeyOwner(@Nonnull Callable<T> task,
                              @Nonnull Object key,
                              @Nullable ExecutionCallback<T> callback);

    /**
     * Submits a task to the specified member. Caller will be notified for the result of the task by
     * {@link ExecutionCallback#onResponse(Object)} or {@link ExecutionCallback#onFailure(Throwable)}.
     *
     * @param task     the task submitted to the specified member
     * @param member   the specified member
     * @param callback callback
     * @param <T>      the result type of callable
     */
    <T> void submitToMember(@Nonnull Callable<T> task,
                            @Nonnull Member member,
                            @Nullable ExecutionCallback<T> callback);

    /**
     * Submits a task to the specified members. Caller will be notified for the result of each task by
     * {@link MultiExecutionCallback#onResponse(Member, Object)}, and when all tasks are completed,
     * {@link MultiExecutionCallback#onComplete(java.util.Map)} will be called.
     *
     * @param task     the task submitted to the specified members
     * @param members  the specified members
     * @param callback callback
     * @param <T>      the result type of callable
     */
    <T> void submitToMembers(@Nonnull Callable<T> task,
                             @Nonnull Collection<Member> members,
                             @Nonnull MultiExecutionCallback callback);

    /**
     * Submits task to the selected members. Caller will be notified for the result of each task by
     * {@link MultiExecutionCallback#onResponse(Member, Object)}, and when all tasks are completed,
     * {@link MultiExecutionCallback#onComplete(java.util.Map)} will be called.
     *
     * @param task           the task submitted to the selected members
     * @param memberSelector memberSelector
     * @param callback       callback
     * @param <T>            the result type of callable
     * @throws java.util.concurrent.RejectedExecutionException if no member is selected
     */
    <T> void submitToMembers(@Nonnull Callable<T> task,
                             @Nonnull MemberSelector memberSelector,
                             @Nonnull MultiExecutionCallback callback);

    /**
     * Submits task to all the cluster members. Caller will be notified for the result of the each task by
     * {@link MultiExecutionCallback#onResponse(Member, Object)}, and when all tasks are completed,
     * {@link MultiExecutionCallback#onComplete(java.util.Map)} will be called.
     *
     * @param task     the task submitted to all the cluster members
     * @param callback callback
     * @param <T>      the result type of callable
     */
    <T> void submitToAllMembers(@Nonnull Callable<T> task,
                                @Nonnull MultiExecutionCallback callback);

    /**
     * Returns local statistics related to this executor service.
     *
     * @return local statistics related to this executor service
     */
    LocalExecutorStats getLocalExecutorStats();
}
