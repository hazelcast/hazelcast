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

package com.hazelcast.spi.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.serialization.Data;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.spi.impl.AbstractInvocationFuture.wrapOrPeel;

/**
 * An extension to {@link CompletableFuture} supporting a {@code joinInternal()}
 * variant with more relaxed exception throwing conventions than {@link #join()}
 * and the ability to override default executor. This is the base class for all
 * {@link CompletableFuture} implementations in Hazelcast.
 *
 * <p>Dependent computation stages can be registered with default, async (when
 * using methods with the {@code Async} suffix) or custom executor (when using
 * methods with the {@code Async} suffix and an {@link Executor} argument).
 * Both default and async execution use the default async executor, which is
 * either the {@link ForkJoinPool#commonPool()} or, in case it does not support
 * parallelism of at least 2, a thread-per-task executor.
 *
 * <p>This class provides static factory methods for more specific implementations
 * supporting custom default async executor or deserialization of completion value.
 */
@SuppressWarnings("checkstyle:methodcount")
public class InternalCompletableFuture<V> extends CompletableFuture<V> {

    public Executor defaultExecutor() {
        return ConcurrencyUtil.getDefaultAsyncExecutor();
    }

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
            throw ExceptionUtil.sneakyThrow(wrapOrPeel(cause));
        }
    }

    @Override
    public <U> CompletableFuture<U> thenApply(Function<? super V, ? extends U> fn) {
        return super.thenApplyAsync(fn, defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super V, ? extends U> fn) {
        return super.thenApplyAsync(fn, defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> thenAccept(Consumer<? super V> action) {
        return super.thenAcceptAsync(action, defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super V> action) {
        return super.thenAcceptAsync(action, defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> thenRun(Runnable action) {
        return super.thenRunAsync(action, defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action) {
        return super.thenRunAsync(action, defaultExecutor());
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombine(CompletionStage<? extends U> other,
                                                     BiFunction<? super V, ? super U, ? extends V1> fn) {
        return super.thenCombineAsync(other, fn, defaultExecutor());
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombineAsync(CompletionStage<? extends U> other,
                                                          BiFunction<? super V, ? super U, ? extends V1> fn) {
        return super.thenCombineAsync(other, fn,  defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other,
                                                      BiConsumer<? super V, ? super U> action) {
        return super.thenAcceptBothAsync(other, action, defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                           BiConsumer<? super V, ? super U> action) {
        return super.thenAcceptBothAsync(other, action, defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return super.runAfterBothAsync(other, action, defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return super.runAfterBothAsync(other, action, defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return super.applyToEitherAsync(other, fn, defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return super.applyToEitherAsync(other, fn, defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> acceptEither(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return super.acceptEitherAsync(other, action, defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return super.acceptEitherAsync(other, action, defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return super.runAfterEitherAsync(other, action, defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return super.runAfterEitherAsync(other, action, defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<U> thenCompose(Function<? super V, ? extends CompletionStage<U>> fn) {
        return super.thenComposeAsync(fn, defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn) {
        return super.thenComposeAsync(fn, defaultExecutor());
    }

    @Override
    public CompletableFuture<V> whenComplete(BiConsumer<? super V, ? super Throwable> action) {
        return super.whenCompleteAsync(action, defaultExecutor());
    }

    @Override
    public CompletableFuture<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action) {
        return super.whenCompleteAsync(action, defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<U> handle(BiFunction<? super V, Throwable, ? extends U> fn) {
        return super.handleAsync(fn, defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn) {
        return super.handleAsync(fn, defaultExecutor());
    }

    /**
     * Creates a new {@code InternalCompletableFuture} that is already completed with the value given
     * as argument as its completion value.
     *
     * @param result    completion value
     * @return          a completed future with the given {@code result} as its completion value.
     */
    public static <V> InternalCompletableFuture<V> newCompletedFuture(Object result) {
        InternalCompletableFuture future = new InternalCompletableFuture();
        future.complete(result);
        return future;
    }

    /**
     * Creates a new {@code InternalCompletableFuture} that is already completed with the value given
     * as argument as its completion value. If the given {@code result} is of type {@link Data}, then
     * it is deserialized before being passed on as argument to a {@code Function}, {@code Consumer},
     * {@code BiFunction} or {@code BiConsumer} further computation stage or before being returned
     * from one of the methods which return the future's value in a blocking way.
     *
     * @param result                the result of the completed future.
     * @param serializationService  instance of {@link SerializationService}
     * @return                      a new {@code InternalCompletableFuture} completed with the given value
     *                              as result, optionally deserializing the completion value
     */
    public static <V> InternalCompletableFuture<V> newCompletedFuture(Object result,
                                                               @Nonnull SerializationService serializationService) {
        InternalCompletableFuture future = new DeserializingCompletableFuture(serializationService, true);
        future.complete(result);
        return future;
    }

    /**
     * Creates a new {@code InternalCompletableFuture} that is already completed with the value given
     * as argument as its completion value and the provided {@code defaultAsyncExecutor} as the default
     * executor for execution of next stages registered with default async methods (i.e. *Async methods
     * which do not have an explicit {@link Executor} argument).
     *
     * @param result                the result of the completed future.
     * @param defaultAsyncExecutor  the executor to use for execution of next computation
     *                              stages, when registered with default async methods
     * @return                      a new {@code InternalCompletableFuture} completed with the given value
     *                              as result
     */
    public static <V> InternalCompletableFuture<V> newCompletedFuture(Object result,
                                                               @Nonnull Executor defaultAsyncExecutor) {
        DeserializingCompletableFuture future = new DeserializingCompletableFuture(defaultAsyncExecutor);
        future.complete(result);
        return future;
    }

    /**
     * Creates a new {@code InternalCompletableFuture} that is already completed with the value given
     * as argument as its completion value and the provided {@code defaultAsyncExecutor} as the default
     * executor for execution of next stages registered with default async methods (i.e. *Async methods
     * which do not have an explicit {@link Executor} argument). If the given {@code result} is of type
     * {@link Data}, then it is deserialized before being passed on as argument to a {@code Function},
     * {@code Consumer}, {@code BiFunction} or {@code BiConsumer} further computation stage or before
     * being returned from one of the methods which return the future's value in a blocking way.
     *
     * @param result                the result of the completed future.
     * @param defaultAsyncExecutor  the executor to use for execution of next computation
     *                              stages, when registered with default async methods
     * @return                      a new {@code InternalCompletableFuture} completed with the given value
     *                              as result
     */
    public static <V> InternalCompletableFuture<V> newCompletedFuture(Object result,
                                                               @Nonnull SerializationService serializationService,
                                                               @Nonnull Executor defaultAsyncExecutor) {
        DeserializingCompletableFuture future = new DeserializingCompletableFuture(serializationService,
                defaultAsyncExecutor, true);
        future.complete(result);
        return future;
    }

    /**
     * Creates a new {@code InternalCompletableFuture} that is completed exceptionally with the
     * given {@code Throwable}.
     *
     * @param t     the {@code Throwable} with which the new future is completed
     * @return      a new {@code InternalCompletableFuture} that is completed exceptionally
     */
    public static <V> InternalCompletableFuture<V> completedExceptionally(@Nonnull Throwable t) {
        InternalCompletableFuture future = new InternalCompletableFuture();
        future.completeExceptionally(t);
        return future;
    }

    /**
     * Creates a new {@code InternalCompletableFuture} that is completed exceptionally with the
     * given {@code Throwable} and uses the given {@code Executor} as default executor for
     * next stages registered with default async methods.
     *
     * @param t                     the {@code Throwable} with which the new future is completed
     * @param defaultAsyncExecutor  the executor to use for execution of next computation
     *                              stages, when registered with default async methods
     * @return                      a new {@code InternalCompletableFuture} that is completed exceptionally
     */
    public static <V> InternalCompletableFuture<V> completedExceptionally(@Nonnull Throwable t,
                                                                   @Nonnull Executor defaultAsyncExecutor) {
        DeserializingCompletableFuture future = new DeserializingCompletableFuture(defaultAsyncExecutor);
        future.completeExceptionally(t);
        return future;
    }

    /**
     * Creates a new incomplete {@code InternalCompletableFuture} that uses the given {@code Executor}
     * as default executor for next stages registered with default async methods.
     *
     * @param defaultAsyncExecutor  the executor to use for execution of next computation
     *                              stages, when registered with default async methods
     * @return                      a new {@code InternalCompletableFuture}
     */
    public static <V> InternalCompletableFuture<V> withExecutor(@Nonnull Executor defaultAsyncExecutor) {
        return new DeserializingCompletableFuture<>(defaultAsyncExecutor);
    }

    /**
     * Creates a new {@code InternalCompletableFuture} that delegates to the given {@code future} and
     * deserializes its completion value of type {@link Data}.
     *
     * @param serializationService  serialization service
     * @param future                the {@code InternalCompletableFuture<Data>} to be decorated.
     * @return                      a new {@code InternalCompletableFuture}
     */
    public static <V> InternalCompletableFuture<V> newDelegatingFuture(@Nonnull SerializationService serializationService,
                                                                       @Nonnull InternalCompletableFuture<Data> future) {
        return new DelegatingCompletableFuture<>(serializationService, future);
    }

    /**
     *
     * @param future
     * @return  a {@link BiConsumer} to be used with {@link CompletableFuture#whenComplete(BiConsumer)} and variants
     *          that completes the {@code future} given as argument normally or exceptionally, depending on whether
     *          {@code Throwable} argument is {@code null}
     */
    public static <U> BiConsumer<U, ? super Throwable> completingCallback(CompletableFuture<U> future) {
        return (BiConsumer<U, Throwable>) (u, throwable) -> {
            if (throwable == null) {
                future.complete(u);
            } else {
                future.completeExceptionally(throwable);
            }
        };
    }
}
