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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.ConcurrencyUtil;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Decorates {@link InternalCompletableFuture} to supply:
 * <ul>
 *     <li>optional value deserialization</li>
 *     <li>custom default async executor</li>
 * </ul>
 */
@SuppressWarnings("checkstyle:methodcount")
public class DeserializingCompletableFuture<V> extends InternalCompletableFuture<V> {

    /**
     * Reference to serialization service; can be {@code null} when {@code deserialize} is {@code false}
     */
    private final InternalSerializationService serializationService;
    /**
     * Default executor for execution of callbacks registered with async methods without
     * explicit {@link Executor} argument (eg. {@link #whenCompleteAsync(BiConsumer)}.
     */
    private final Executor defaultAsyncExecutor;
    /**
     * When {@code true}, a completion value of type {@link Data} will be deserialized
     * before returned from one of the blocking results getter methods ({@link #get()}, {@link #join()} etc)
     * or before passed as argument to callbacks such as {@link #thenAccept(Consumer)}.
     */
    private final boolean deserialize;

    public DeserializingCompletableFuture() {
        this(null, ConcurrencyUtil.getDefaultAsyncExecutor(), false);
    }

    public DeserializingCompletableFuture(Executor defaultAsyncExecutor) {
        this(null, defaultAsyncExecutor, false);
    }

    public DeserializingCompletableFuture(SerializationService serializationService, boolean deserialize) {
        this(serializationService, ConcurrencyUtil.getDefaultAsyncExecutor(), deserialize);
    }

    public DeserializingCompletableFuture(SerializationService serializationService, Executor defaultAsyncExecutor,
                                          boolean deserialize) {
        this.serializationService = (InternalSerializationService) serializationService;
        this.defaultAsyncExecutor = defaultAsyncExecutor;
        this.deserialize = deserialize;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        return resolve(super.get());
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return resolve(super.get(timeout, unit));
    }

    @Override
    public V getNow(V valueIfAbsent) {
        V value = super.getNow(valueIfAbsent);
        return (deserialize && value instanceof Data) ? serializationService.toObject(value)
                : value;
    }

    @Override
    public V join() {
        return resolve(super.join());
    }

    @Override
    public V joinInternal() {
        return resolve(super.joinInternal());
    }

    @Override
    public <U> CompletableFuture<U> thenApply(Function<? super V, ? extends U> fn) {
        return super.thenApply(new DeserializingFunction<>(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super V, ? extends U> fn) {
        return super.thenApplyAsync(new DeserializingFunction<>(fn), defaultAsyncExecutor);
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super V, ? extends U> fn, Executor executor) {
        return super.thenApplyAsync(new DeserializingFunction<>(fn), executor);
    }

    @Override
    public CompletableFuture<Void> thenAccept(Consumer<? super V> action) {
        return super.thenAccept(new DeserializingConsumer<>(action));
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super V> action) {
        return super.thenAcceptAsync(new DeserializingConsumer<>(action), defaultAsyncExecutor);
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super V> action, Executor executor) {
        return super.thenAcceptAsync(new DeserializingConsumer<>(action), executor);
    }

    @Override
    public CompletableFuture<Void> thenRun(Runnable action) {
        return super.thenRun(action);
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action) {
        return super.thenRunAsync(action, defaultAsyncExecutor);
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
        return super.thenRunAsync(action, executor);
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombine(CompletionStage<? extends U> other,
                                                     BiFunction<? super V, ? super U, ? extends V1> fn) {
        return super.thenCombine(other, new DeserializingBiFunction<>(fn));
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombineAsync(CompletionStage<? extends U> other,
                                                          BiFunction<? super V, ? super U, ? extends V1> fn) {
        return super.thenCombineAsync(other, new DeserializingBiFunction<>(fn), defaultAsyncExecutor);
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombineAsync(CompletionStage<? extends U> other,
                                                          BiFunction<? super V, ? super U, ? extends V1> fn, Executor executor) {
        return super.thenCombineAsync(other, new DeserializingBiFunction<>(fn), executor);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other,
                                                      BiConsumer<? super V, ? super U> action) {
        return super.thenAcceptBoth(other, new DeserializingBiConsumer<>(action));
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                           BiConsumer<? super V, ? super U> action) {
        return super.thenAcceptBothAsync(other, new DeserializingBiConsumer<>(action), defaultAsyncExecutor);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                           BiConsumer<? super V, ? super U> action, Executor executor) {
        return super.thenAcceptBothAsync(other, new DeserializingBiConsumer<>(action), executor);
    }

    @Override
    public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return super.runAfterBoth(other, action);
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return super.runAfterBothAsync(other, action, defaultAsyncExecutor);
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return super.runAfterBothAsync(other, action, executor);
    }

    @Override
    public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return super.applyToEither(other, new DeserializingFunction<>(fn));
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return super.applyToEitherAsync(other, new DeserializingFunction<>(fn), defaultAsyncExecutor);
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn,
                                                       Executor executor) {
        return super.applyToEitherAsync(other, new DeserializingFunction<>(fn), executor);
    }

    @Override
    public CompletableFuture<Void> acceptEither(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return super.acceptEither(other, new DeserializingConsumer<>(action));
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return super.acceptEitherAsync(other, new DeserializingConsumer<>(action), defaultAsyncExecutor);
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action,
                                                     Executor executor) {
        return super.acceptEitherAsync(other, new DeserializingConsumer<>(action), executor);
    }

    @Override
    public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return super.runAfterEither(other, action);
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return super.runAfterEitherAsync(other, action, defaultAsyncExecutor);
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return super.runAfterEitherAsync(other, action, executor);
    }

    @Override
    public <U> CompletableFuture<U> thenCompose(Function<? super V, ? extends CompletionStage<U>> fn) {
        return super.thenCompose(new DeserializingFunction<>(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn) {
        return super.thenComposeAsync(new DeserializingFunction<>(fn), defaultAsyncExecutor);
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn, Executor executor) {
        return super.thenComposeAsync(new DeserializingFunction<>(fn), executor);
    }

    @Override
    public CompletableFuture<V> whenComplete(BiConsumer<? super V, ? super Throwable> action) {
        if (!deserialize) {
            return super.whenComplete(new DeserializingBiConsumer<>(action));
        } else {
            return new DelegatingCompletableFuture<>(serializationService,
                    super.whenComplete(new DeserializingBiConsumer<>(action)));
        }
    }

    @Override
    public CompletableFuture<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action) {
        if (!deserialize) {
            return super.whenCompleteAsync(new DeserializingBiConsumer<>(action), defaultAsyncExecutor);
        } else {
            return new DelegatingCompletableFuture<>(serializationService,
                    super.whenCompleteAsync(new DeserializingBiConsumer<>(action)
                            , defaultAsyncExecutor));
        }
    }

    @Override
    public CompletableFuture<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action, Executor executor) {
        if (!deserialize) {
            return super.whenCompleteAsync(new DeserializingBiConsumer<>(action), executor);
        } else {
            return new DelegatingCompletableFuture<V>(serializationService,
                    super.whenCompleteAsync(new DeserializingBiConsumer<>(action), executor));
        }
    }

    @Override
    public <U> CompletableFuture<U> handle(BiFunction<? super V, Throwable, ? extends U> fn) {
        return super.handle(new DeserializingBiFunction<>(fn));
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn) {
        return super.handleAsync(new DeserializingBiFunction<>(fn), defaultAsyncExecutor);
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
        return super.handleAsync(new DeserializingBiFunction<>(fn), executor);
    }

    @Override
    public CompletableFuture<V> exceptionally(Function<Throwable, ? extends V> fn) {
        if (!deserialize) {
            return super.exceptionally(fn);
        } else {
            return new DelegatingCompletableFuture<>(serializationService, super.exceptionally(fn));
        }
    }

    private V resolve(Object object) {
        if (deserialize && object instanceof Data) {
            // we need to deserialize.
            Data data = (Data) object;
            object = serializationService.toObject(data);

            //todo do we need to call dispose data here
            serializationService.disposeData(data);
        }
        return (V) object;
    }

    class DeserializingFunction<E, R> implements Function<E, R> {
        private final Function<E, R> delegate;

        DeserializingFunction(Function<E, R> delegate) {
            if (delegate == null) {
                throw new NullPointerException();
            }
            this.delegate = delegate;
        }

        @Override
        public R apply(E e) {
            return delegate.apply(deserialize ? serializationService.toObject(e) : e);
        }
    }

    class DeserializingConsumer<E> implements Consumer<E> {
        private final Consumer<E> delegate;

        DeserializingConsumer(Consumer<E> delegate) {
            if (delegate == null) {
                throw new NullPointerException();
            }
            this.delegate = delegate;
        }

        @Override
        public void accept(E e) {
            delegate.accept(deserialize ? serializationService.toObject(e) : e);
        }
    }

    class DeserializingBiFunction<T, U, R> implements BiFunction<T, U, R> {
        private final BiFunction<T, U, R> delegate;

        DeserializingBiFunction(BiFunction<T, U, R> delegate) {
            if (delegate == null) {
                throw new NullPointerException();
            }
            this.delegate = delegate;
        }

        @Override
        public R apply(T t, U u) {
            return delegate.apply(deserialize ? serializationService.toObject(t) : t,
                    deserialize ? serializationService.toObject(u) : u);
        }
    }

    class DeserializingBiConsumer<T, U> implements BiConsumer<T, U> {
        private final BiConsumer<T, U> delegate;

        DeserializingBiConsumer(BiConsumer<T, U> delegate) {
            if (delegate == null) {
                throw new NullPointerException();
            }
            this.delegate = delegate;
        }

        @Override
        public void accept(T t, U u) {
            delegate.accept(deserialize ? serializationService.toObject(t) : t,
                    deserialize ? serializationService.toObject(u) : u);
        }
    }
}
