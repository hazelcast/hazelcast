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

package com.hazelcast.durableexecutor;

import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.DistributedObject;

import javax.annotation.Nonnull;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

/**
 * Durable implementation of {@link ExecutorService}.
 * DurableExecutor provides additional methods like executing tasks on a member who is owner of a specific key
 * DurableExecutor also provides a way to retrieve the result of an execution with the given taskId.
 *
 * @see ExecutorService
 * <p>
 * Supports split brain protection {@link SplitBrainProtectionConfig} since 3.10 in cluster versions 3.10 and higher.
 */
public interface DurableExecutorService extends ExecutorService, DistributedObject {

    /**
     * Submits a value-returning task for execution and returns a
     * Future representing the pending results of the task. The
     * Future's {@code get} method will return the task's result upon
     * successful completion.
     * <p>
     * If you would like to immediately block waiting
     * for a task, you can use constructions of the form
     * {@code result = exec.submit(aCallable).get();}
     * <p>Note: The {@link Executors} class includes a set of methods
     * that can convert some other common closure-like objects,
     * for example, {@link java.security.PrivilegedAction} to
     * {@link Callable} form so they can be submitted.
     *
     * @param task the task to submit
     * @param <T>  the type of the task's result
     * @return a Future representing pending completion of the task
     * @throws RejectedExecutionException if the task cannot be
     *                                    scheduled for execution
     * @throws NullPointerException       if the task is null
     */
    @Nonnull
    <T> DurableExecutorServiceFuture<T> submit(@Nonnull Callable<T> task);

    /**
     * Submits a Runnable task for execution and returns a Future
     * representing that task. The Future's {@code get} method will
     * return the given result upon successful completion.
     *
     * @param task   the task to submit
     * @param result the result to return
     * @param <T>    the type of the result
     * @return a Future representing pending completion of the task
     * @throws RejectedExecutionException if the task cannot be
     *                                    scheduled for execution
     * @throws NullPointerException       if the task is null
     */
    @Nonnull
    <T> DurableExecutorServiceFuture<T> submit(@Nonnull Runnable task, T result);

    /**
     * Submits a Runnable task for execution and returns a Future
     * representing that task. The Future's {@code get} method will
     * return {@code null} upon <em>successful</em> completion.
     *
     * @param task the task to submit
     * @return a Future representing pending completion of the task
     * @throws RejectedExecutionException if the task cannot be
     *                                    scheduled for execution
     * @throws NullPointerException       if the task is null
     */
    @Override
    @Nonnull
    DurableExecutorServiceFuture<?> submit(@Nonnull Runnable task);

    /**
     * Retrieves the result with the given taskId
     *
     * @param taskId combination of partitionId and sequence
     * @param <T>    the type of the task's result
     * @return a Future representing pending completion of the task
     */
    <T> Future<T> retrieveResult(long taskId);

    /**
     * Disposes the result with the given taskId
     *
     * @param taskId combination of partitionId and sequence
     */
    void disposeResult(long taskId);

    /**
     * Retrieves and disposes the result with the given taskId
     *
     * @param taskId combination of partitionId and sequence
     * @param <T>    the type of the task's result
     * @return a Future representing pending completion of the task
     */
    <T> Future<T> retrieveAndDisposeResult(long taskId);

    /**
     * Executes a task on the owner of the specified key.
     *
     * @param command a task executed on the owner of the specified key
     * @param key     the specified key
     */
    void executeOnKeyOwner(@Nonnull Runnable command,
                           @Nonnull Object key);

    /**
     * Submits a task to the owner of the specified key and returns a Future
     * representing that task.
     *
     * @param task task submitted to the owner of the specified key
     * @param key  the specified key
     * @param <T>  the return type of the task
     * @return a Future representing pending completion of the task
     */
    <T> DurableExecutorServiceFuture<T> submitToKeyOwner(@Nonnull Callable<T> task,
                                                         @Nonnull Object key);

    /**
     * Submits a task to the owner of the specified key and returns a Future
     * representing that task.
     *
     * @param task task submitted to the owner of the specified key
     * @param key  the specified key
     * @return a Future representing pending completion of the task
     */
    DurableExecutorServiceFuture<?> submitToKeyOwner(@Nonnull Runnable task,
                                                     @Nonnull Object key);
}
