/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;

/**
 * An extension to {@link CompletableFuture} supporting a {@code joinInternal()}
 * variant with more relaxed exception throwing conventions.
 *
 * This is the base class for all internally used {@link CompletableFuture}s.
 *
 * Does not perform optional deserialization. Uses {@link ForkJoinPool#commonPool()}
 * or thread-per-task executor as default async executor, as {@link CompletableFuture} does.
 *
 * Provides static factory methods for more specific implementations supporting custom
 * default async executor or deserialization of completion value.
 */
public class InternalCompletableFuture<V> extends CompletableFuture<V> {

    /**
     * Similarly to {@link #join()}, returns the value when complete or throws an unchecked exception if
     * completed exceptionally. Unlike {@link #join()}, checked exceptions are not wrapped in {@link CompletionException};
     * rather they are wrapped in {@link com.hazelcast.core.HazelcastException}s.
     *
     * @return the result
     */
    public V joinInternal() {
        try {
            return join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            throw AbstractInvocationFuture.wrapOrPeel(cause);
        }
    }

    /**
     * Creates a new {@code InternalCompletableFuture} that is already completed with the value given
     * as argument as its completion value.
     *
     * @param result    completion value
     * @return          a completed future with the given {@code result} as its completion value.
     */
    public static InternalCompletableFuture newCompletedFuture(Object result) {
        InternalCompletableFuture future = new InternalCompletableFuture();
        future.complete(result);
        return future;
    }

    /**
     * Creates a new {@code InternalCompletableFuture} that is already completed with the value given
     * as argument as its completion value. If the given {@code result} is of type {@link Data}, then
     * it is deserialized before being passed on as argument to a {@code Function}, {@code Consumer},
     * {@code BiFunction} or {@code BiConsumer} callback or before being returned from one of the methods
     * which return the future's value in a blocking way.
     *
     * @param result                the result of the completed future.
     * @param serializationService  instance of {@link SerializationService}
     * @return                      a new {@code InternalCompletableFuture} completed with the given value
     *                              as result, optionally deserializing the completion value
     */
    public static InternalCompletableFuture newCompletedFuture(Object result,
                                                               @Nonnull SerializationService serializationService) {
        InternalCompletableFuture future = new DeserializingCompletableFuture(serializationService, true);
        future.complete(result);
        return future;
    }

    /**
     * Creates a new {@code InternalCompletableFuture} that is already completed with the value given
     * as argument as its completion value and the provided {@code defaultAsyncExecutor} as the default
     * executor for execution of callbacks registered with default async methods (i.e. *Async methods
     * which do not have an explicit {@link Executor} argument).
     *
     * @param result                the result of the completed future.
     * @param serializationService  default async callbacks executor
     * @return                      a new {@code InternalCompletableFuture} completed with the given value
     *                              as result
     */
    public static InternalCompletableFuture newCompletedFuture(Object result,
                                                               @Nonnull Executor defaultAsyncExecutor) {
        DeserializingCompletableFuture future = new DeserializingCompletableFuture(defaultAsyncExecutor);
        future.complete(result);
        return future;
    }

    public static InternalCompletableFuture newCompletedFuture(Object result,
                                                               @Nonnull SerializationService serializationService,
                                                               @Nonnull Executor defaultAsyncExecutor) {
        DeserializingCompletableFuture future = new DeserializingCompletableFuture(serializationService,
                defaultAsyncExecutor, true);
        future.complete(result);
        return future;
    }

    public static InternalCompletableFuture completedExceptionally(@Nonnull Throwable t) {
        InternalCompletableFuture future = new InternalCompletableFuture();
        future.completeExceptionally(t);
        return future;
    }

    public static InternalCompletableFuture completedExceptionally(@Nonnull Throwable t,
                                                                   @Nonnull Executor defaultAsyncExecutor) {
        DeserializingCompletableFuture future = new DeserializingCompletableFuture(defaultAsyncExecutor);
        future.completeExceptionally(t);
        return future;
    }

    public static <U> InternalCompletableFuture<U> withExecutor(@Nonnull Executor defaultAsyncExecutor) {
        return new DeserializingCompletableFuture<>(defaultAsyncExecutor);
    }

    public static <U> InternalCompletableFuture<U> newDelegatingFuture(@Nonnull SerializationService serializationService,
                                                                       @Nonnull InternalCompletableFuture<Data> future) {
        return new DelegatingCompletableFuture<U>(serializationService, future);
    }

    /**
     *
     * @param future
     * @param <U>
     * @return  a {@link BiConsumer} to be used with {@link CompletableFuture#whenComplete(BiConsumer)} and variants
     *          that completes the {@code future} given as argument normally or exceptionally, depending on whether
     *          {@code Throwable} argument is {@code null}
     */
    public static <U> BiConsumer<U, ? super Throwable> completingCallback(CompletableFuture future) {
        return (BiConsumer<U, Throwable>) (u, throwable) -> {
            if (throwable == null) {
                future.complete(u);
            } else {
                future.completeExceptionally(throwable);
            }
        };
    }
}
