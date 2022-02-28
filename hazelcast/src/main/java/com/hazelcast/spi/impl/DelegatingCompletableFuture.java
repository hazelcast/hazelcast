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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static java.util.Objects.requireNonNull;

/**
 * A {@link InternalCompletableFuture} implementation that delegates the real logic to an underlying
 * {@link InternalCompletableFuture} and decorates it with additional behavior:
 * <ol>
 * <li>change the returned value by setting the result</li>
 * <li>caching the deserialized content so that a deserialization only happens once. This should be used with
 * care since this could lead to unexpected sharing of object instances if the same future is shared between
 * threads.
 * </li>
 * </ol>
 *
 * Even though the wrapped future may be completed normally, it is possible that
 * a {@link HazelcastSerializationException} thrown when deserializing the value
 * will result in this future being completed exceptionally. A deserialization
 * failure makes this future being considered to complete exceptionally,
 * therefore futures from dependent stages will be completed with a
 * HazelcastSerializationException (unless the dependent stage transforms
 * the outcome).
 *
 * @param <V>
 */
@SuppressWarnings("checkstyle:methodcount")
public class DelegatingCompletableFuture<V> extends InternalCompletableFuture<V> {

    protected static final Object VOID = new Object() {
        @Override
        public String toString() {
            return "void";
        }
    };

    private static final AtomicReferenceFieldUpdater<DelegatingCompletableFuture, Object> DESERIALIZED_VALUE
            = AtomicReferenceFieldUpdater.newUpdater(DelegatingCompletableFuture.class,
            Object.class, "deserializedValue");

    protected final CompletableFuture future;
    protected final InternalSerializationService serializationService;
    protected final Object result;

    protected volatile Object deserializedValue = VOID;

    public DelegatingCompletableFuture(@Nonnull SerializationService serializationService,
                                       @Nonnull CompletableFuture future) {
        this(serializationService, future, null);
    }

    public DelegatingCompletableFuture(@Nonnull SerializationService serializationService,
                                       @Nonnull CompletableFuture future,
                                       V result) {
        this(serializationService, future, result, true);
    }

    protected DelegatingCompletableFuture(@Nonnull SerializationService serializationService,
                                          @Nonnull CompletableFuture future,
                                          V result,
                                          boolean listenFutureCompletion) {
        this.future = future;
        this.serializationService = (InternalSerializationService) serializationService;
        this.result = result;
        if (listenFutureCompletion) {
            this.future.whenComplete((v, t) -> completeSuper(v, (Throwable) t));
        }
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        try {
            return resolve(future.get());
        } catch (HazelcastSerializationException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            return resolve(future.get(timeout, unit));
        } catch (HazelcastSerializationException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public V getNow(V valueIfAbsent) {
        // if there is an explicit value set, we use that
        if (result != null) {
            return (V) result;
        }

        // if there already is a deserialized value set, use it.
        if (deserializedValue != VOID) {
            return (V) deserializedValue;
        }

        // otherwise, do not cache the value returned from future.getNow
        // because it might be the default valueIfAbsent
        Object value = future.getNow(valueIfAbsent);
        try {
            return (value instanceof Data)
                    ? serializationService.toObject(value) : (V) value;
        } catch (HazelcastSerializationException e) {
            throw new CompletionException(e);
        }
    }

    @Override
    public V join() {
        try {
            return resolve(future.join());
        } catch (HazelcastSerializationException e) {
            throw new CompletionException(e);
        }
    }

    @Override
    public V joinInternal() {
        if (future instanceof InternalCompletableFuture) {
            return resolve(((InternalCompletableFuture) future).joinInternal());
        } else {
            try {
                return resolve(future.join());
            } catch (CompletionException e) {
                Throwable cause = e.getCause();
                throw sneakyThrow(AbstractInvocationFuture.wrapOrPeel(cause));
            }
        }
    }

    // public for testing
    public Future getDelegate() {
        return future;
    }

    // Overriding this method means you also have to override getNow
    protected V resolve(Object object) {
        // if there is an explicit value set, we use that
        if (result != null) {
            return (V) result;
        }

        // if there already is a deserialized value set, use it.
        if (deserializedValue != VOID) {
            return (V) deserializedValue;
        }

        if (object instanceof Data) {
            // we need to deserialize.
            Data data = (Data) object;
            object = serializationService.toObject(data);
            serializationService.disposeData(data);

            object = cacheDeserializedValue(object);
        }
        return (V) object;
    }

    protected Object cacheDeserializedValue(Object object) {
        for (; ; ) {
            Object current = deserializedValue;
            if (current != VOID) {
                object = current;
                break;
            } else if (DESERIALIZED_VALUE.compareAndSet(this, VOID, object)) {
                break;
            }
        }

        return object;
    }

    @Override
    public boolean isDone() {
        return future.isDone();
    }

    @Override
    public boolean complete(V value) {
        boolean triggered = future.complete(value);
        if (triggered) {
            super.complete(value);
        }
        return triggered;
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        boolean triggered = future.completeExceptionally(ex);
        if (triggered) {
            super.completeExceptionally(ex);
        }
        return triggered;
    }

    @Override
    public <U> CompletableFuture<U> thenApply(Function<? super V, ? extends U> fn) {
        return future.thenApply(new DeserializingFunction<>(serializationService, fn));
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super V, ? extends U> fn) {
        return future.thenApplyAsync(new DeserializingFunction<>(serializationService, fn));
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super V, ? extends U> fn, Executor executor) {
        return future.thenApplyAsync(new DeserializingFunction<>(serializationService, fn), executor);
    }

    @Override
    public CompletableFuture<Void> thenAccept(Consumer<? super V> action) {
        return future.thenAccept(new DeserializingConsumer<>(serializationService, action));
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super V> action) {
        return future.thenAcceptAsync(new DeserializingConsumer<>(serializationService, action));
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super V> action, Executor executor) {
        return future.thenAcceptAsync(new DeserializingConsumer<>(serializationService, action), executor);
    }

    @Override
    public CompletableFuture<Void> thenRun(Runnable action) {
        return future.thenRun(new DeserializingRunnable(serializationService, action));
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action) {
        return future.thenRunAsync(new DeserializingRunnable(serializationService, action));
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
        return future.thenRunAsync(new DeserializingRunnable(serializationService, action), executor);
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombine(CompletionStage<? extends U> other,
                                                     BiFunction<? super V, ? super U, ? extends V1> fn) {
        return future.thenCombine(other, new DeserializingBiFunction<>(serializationService, fn));
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombineAsync(CompletionStage<? extends U> other,
                                                          BiFunction<? super V, ? super U, ? extends V1> fn) {
        return future.thenCombineAsync(other, new DeserializingBiFunction<>(serializationService, fn));
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombineAsync(CompletionStage<? extends U> other,
                                                          BiFunction<? super V, ? super U, ? extends V1> fn, Executor executor) {
        return future.thenCombineAsync(other, new DeserializingBiFunction<>(serializationService, fn), executor);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other,
                                                      BiConsumer<? super V, ? super U> action) {
        return future.thenAcceptBoth(other, new DeserializingBiConsumer<>(serializationService, action));
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                           BiConsumer<? super V, ? super U> action) {
        return future.thenAcceptBothAsync(other, new DeserializingBiConsumer<>(serializationService, action));
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                           BiConsumer<? super V, ? super U> action, Executor executor) {
        return future.thenAcceptBothAsync(other, new DeserializingBiConsumer<>(serializationService, action), executor);
    }

    @Override
    public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return future.runAfterBoth(other, new DeserializingRunnable(serializationService, action));
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return future.runAfterBothAsync(other, new DeserializingRunnable(serializationService, action));
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return future.runAfterBothAsync(other, new DeserializingRunnable(serializationService, action), executor);
    }

    @Override
    public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return future.applyToEither(other, new DeserializingFunction<>(serializationService, fn));
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return future.applyToEitherAsync(other, new DeserializingFunction<>(serializationService, fn));
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn,
                                                       Executor executor) {
        return future.applyToEitherAsync(other, new DeserializingFunction<>(serializationService, fn), executor);
    }

    @Override
    public CompletableFuture<Void> acceptEither(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return future.acceptEither(other, new DeserializingConsumer<>(serializationService, action));
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return future.acceptEitherAsync(other, new DeserializingConsumer<>(serializationService, action));
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action,
                                                     Executor executor) {
        return future.acceptEitherAsync(other, new DeserializingConsumer<>(serializationService, action), executor);
    }

    @Override
    public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return future.runAfterEither(other, new DeserializingRunnable(serializationService, action));
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return future.runAfterEitherAsync(other, new DeserializingRunnable(serializationService, action));
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return future.runAfterEitherAsync(other, new DeserializingRunnable(serializationService, action), executor);
    }

    @Override
    public <U> CompletableFuture<U> thenCompose(Function<? super V, ? extends CompletionStage<U>> fn) {
        return future.thenCompose(new DeserializingFunction<>(serializationService, fn));
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn) {
        return future.thenComposeAsync(new DeserializingFunction<>(serializationService, fn));
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn, Executor executor) {
        return future.thenComposeAsync(new DeserializingFunction<>(serializationService, fn), executor);
    }

    @Override
    public CompletableFuture<V> whenComplete(BiConsumer<? super V, ? super Throwable> action) {
        return new DelegatingCompletableFuture<>(serializationService,
                future.whenComplete(new WhenCompleteBiConsumer(serializationService, action)));
    }

    @Override
    public CompletableFuture<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action) {
        return new DelegatingCompletableFuture<>(serializationService,
                future.whenCompleteAsync(new WhenCompleteBiConsumer(serializationService, action)));
    }

    @Override
    public CompletableFuture<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action, Executor executor) {
        return new DelegatingCompletableFuture<>(serializationService,
                future.whenCompleteAsync(new WhenCompleteBiConsumer(serializationService, action), executor));
    }

    @Override
    public <U> CompletableFuture<U> handle(BiFunction<? super V, Throwable, ? extends U> fn) {
        return future.handle(new HandleBiFunction(serializationService, fn));
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn) {
        return future.handleAsync(new HandleBiFunction<>(serializationService, fn));
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
        return future.handleAsync(new HandleBiFunction<>(serializationService, fn), executor);
    }

    @Override
    public CompletableFuture<V> toCompletableFuture() {
        return this;
    }

    @Override
    public CompletableFuture<V> exceptionally(Function<Throwable, ? extends V> fn) {
        return future.handle(new ExceptionallyBiFunction(serializationService, fn));
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isCompletedExceptionally() {
        // if super is completed, then value deserialization has already happened,
        // so we know if this future is completed exceptionally
        if (super.isDone()) {
            return super.isCompletedExceptionally();
        }
        // otherwise, check the delegate future: if that one is done, try
        // resolve the completion value
        if (future.isDone()) {
            try {
                resolve(future.join());
                return false;
            } catch (Throwable t) {
                return true;
            }
        } else {
            return false;
        }
    }

    @Override
    public void obtrudeValue(V value) {
        future.obtrudeValue(value);
    }

    @Override
    public void obtrudeException(Throwable ex) {
        future.obtrudeException(ex);
    }

    @Override
    public int getNumberOfDependents() {
        return future.getNumberOfDependents();
    }

    @Override
    public String toString() {
        return future.toString();
    }

    // used for testing
    public V getDeserializedValue() {
        return (V) deserializedValue;
    }

    protected void completeSuper(Object value, Throwable t) {
        if (t != null) {
            super.completeExceptionally(t);
        } else {
            try {
                V resolved = resolve(value);
                super.complete(resolved);
            } catch (HazelcastSerializationException e) {
                super.completeExceptionally(e);
            }
        }
    }

    static class DeserializingFunction<E, R> implements Function<E, R> {
        private final SerializationService serializationService;
        private final Function<E, R> delegate;

        DeserializingFunction(SerializationService serializationService, Function<E, R> delegate) {
            requireNonNull(delegate);

            this.serializationService = serializationService;
            this.delegate = delegate;
        }

        @Override
        public R apply(E e) {
            return delegate.apply(serializationService.toObject(e));
        }
    }

    class DeserializingRunnable implements Runnable {
        private final SerializationService serializationService;
        private final Runnable delegate;

        DeserializingRunnable(SerializationService serializationService, Runnable delegate) {
            requireNonNull(delegate);

            this.serializationService = serializationService;
            this.delegate = delegate;
        }

        @Override
        public void run() {
            // deserialize to ensure no HazelcastSerializationException occurs
            serializationService.toObject(future.join());
            delegate.run();
        }
    }

    static class DeserializingConsumer<E> implements Consumer<E> {
        private final SerializationService serializationService;
        private final Consumer<E> delegate;

        DeserializingConsumer(SerializationService serializationService, Consumer<E> delegate) {
            requireNonNull(delegate);

            this.serializationService = serializationService;
            this.delegate = delegate;
        }

        @Override
        public void accept(E e) {
            delegate.accept(serializationService.toObject(e));
        }
    }

    static class DeserializingBiFunction<T, U, R> implements BiFunction<T, U, R> {
        private final SerializationService serializationService;
        private final BiFunction<T, U, R> delegate;

        DeserializingBiFunction(SerializationService serializationService, BiFunction<T, U, R> delegate) {
            requireNonNull(delegate);

            this.serializationService = serializationService;
            this.delegate = delegate;
        }

        @Override
        public R apply(T t, U u) {
            T v1 = serializationService.toObject(t);
            U v2 = serializationService.toObject(u);
            return delegate.apply(v1, v2);
        }
    }

    static class HandleBiFunction<T, U extends Throwable, R> implements BiFunction<T, U, R> {
        private final SerializationService serializationService;
        private final BiFunction<T, U, R> delegate;

        HandleBiFunction(SerializationService serializationService, BiFunction<T, U, R> delegate) {
            requireNonNull(delegate);

            this.serializationService = serializationService;
            this.delegate = delegate;
        }

        @Override
        public R apply(T t, U u) {
            T deserialized = null;
            try {
                deserialized = serializationService.toObject(t);
            } catch (HazelcastSerializationException exc) {
                u = (U) exc;
            }
            return delegate.apply(deserialized, u);
        }
    }

    static class DeserializingBiConsumer<T, U> implements BiConsumer<T, U> {
        private final SerializationService serializationService;
        private final BiConsumer<T, U> delegate;

        DeserializingBiConsumer(SerializationService serializationService, BiConsumer<T, U> delegate) {
            requireNonNull(delegate);

            this.serializationService = serializationService;
            this.delegate = delegate;
        }

        @Override
        public void accept(T t, U u) {
            T v1 = serializationService.toObject(t);
            U v2 = serializationService.toObject(u);
            delegate.accept(v1, v2);
        }
    }

    static class WhenCompleteBiConsumer<E, T extends Throwable> implements BiConsumer<E, T> {
        private final SerializationService serializationService;
        private final BiConsumer<E, T> delegate;

        WhenCompleteBiConsumer(SerializationService serializationService, BiConsumer<E, T> delegate) {
            requireNonNull(delegate);

            this.serializationService = serializationService;
            this.delegate = delegate;
        }

        @Override
        public void accept(E v, T t) {
            E deserialized = null;
            try {
                deserialized = serializationService.toObject(v);
            } catch (HazelcastSerializationException exc) {
                t = (T) exc;
            }
            delegate.accept(deserialized, t);
        }
    }

    static class ExceptionallyBiFunction<T, U extends Throwable, R> implements BiFunction<T, U, R> {
        private final SerializationService serializationService;
        private final Function<U, R> delegate;

        ExceptionallyBiFunction(SerializationService serializationService, Function<U, R> delegate) {
            requireNonNull(delegate);

            this.serializationService = serializationService;
            this.delegate = delegate;
        }

        @Override
        public R apply(T t, U u) {
            if (u != null) {
                return delegate.apply(u);
            } else {
                R deserialized;
                try {
                    deserialized = serializationService.toObject(t);
                    return deserialized;
                } catch (HazelcastSerializationException exc) {
                    u = (U) exc;
                    return delegate.apply(u);
                }
            }
        }
    }
}
