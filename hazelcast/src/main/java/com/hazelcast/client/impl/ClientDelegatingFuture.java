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

package com.hazelcast.client.impl;

import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.impl.DelegatingCompletableFuture;

import javax.annotation.Nonnull;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static java.util.Objects.requireNonNull;

/**
 * The Client Delegating Future is used to delegate {@link
 * ClientInvocationFuture} to a user type to be used with further computation stages or
 * {@code get()}. It converts {@link ClientMessage} coming from {@link
 * ClientInvocationFuture} to a user object.
 *
 * @param <V> Value type that the user expects
 */
@SuppressWarnings("checkstyle:methodcount")
public class ClientDelegatingFuture<V> extends DelegatingCompletableFuture<V> {

    private static final AtomicReferenceFieldUpdater<ClientDelegatingFuture, Object> DECODED_RESPONSE =
            AtomicReferenceFieldUpdater.newUpdater(ClientDelegatingFuture.class, Object.class, "decodedResponse");

    final boolean deserializeResponse;
    private final ClientMessageDecoder clientMessageDecoder;
    private volatile Object decodedResponse;

    public ClientDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
                                  SerializationService serializationService,
                                  ClientMessageDecoder clientMessageDecoder, V defaultValue, boolean deserializeResponse) {
        super(serializationService, clientInvocationFuture, defaultValue, false);
        this.decodedResponse = VOID;
        this.clientMessageDecoder = clientMessageDecoder;
        this.deserializeResponse = deserializeResponse;
        this.future.whenComplete((v, t) -> completeSuper(v, (Throwable) t));
    }

    public ClientDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
                                  SerializationService serializationService,
                                  ClientMessageDecoder clientMessageDecoder, V defaultValue) {
        this(clientInvocationFuture, serializationService, clientMessageDecoder, defaultValue, true);
    }

    public ClientDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
                                  SerializationService serializationService,
                                  ClientMessageDecoder clientMessageDecoder) {
        this(clientInvocationFuture, serializationService, clientMessageDecoder, null, true);
    }

    public ClientDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
                                  SerializationService serializationService,
                                  ClientMessageDecoder clientMessageDecoder, boolean deserializeResponse) {
        this(clientInvocationFuture, serializationService, clientMessageDecoder, null, deserializeResponse);
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
            if (value instanceof ClientMessage) {
                return resolve(value);
            } else {
                return (value instanceof Data && deserializeResponse)
                        ? serializationService.toObject(value) : (V) value;
            }
        } catch (HazelcastSerializationException exc) {
            throw new CompletionException(exc);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected V resolve(Object object) {
        if (result != null) {
            return (V) result;
        }

        // if there already is a deserialized value set, use it.
        if (deserializedValue != VOID) {
            return (V) deserializedValue;
        }

        ClientMessage clientMessage = (ClientMessage) object;
        Object decoded = decodeResponse(clientMessage);
        if (deserializeResponse) {
            decoded = serializationService.toObject(decoded);
        }
        decoded = cacheDeserializedValue(decoded);

        return (V) decoded;
    }

    private Object resolveAny(Object o) {
        if (o instanceof ClientMessage) {
            return resolve(o);
        }
        if (deserializeResponse) {
            return serializationService.toObject(o);
        }
        return o;
    }

    private Object decodeResponse(ClientMessage clientMessage) {
        if (decodedResponse != VOID) {
            return decodedResponse;
        }
        //Since frames are read, there should be no need to re-read to client message
        Object newDecodedResponse = clientMessageDecoder.decodeClientMessage(clientMessage);

        DECODED_RESPONSE.compareAndSet(this, VOID, newDecodedResponse);
        return newDecodedResponse;
    }

    protected ClientInvocationFuture getFuture() {
        return (ClientInvocationFuture) future;
    }

    @Override
    public <U> CompletableFuture<U> thenApply(Function<? super V, ? extends U> fn) {
        return future.thenApplyAsync(new DeserializingFunction<>(fn), defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super V, ? extends U> fn) {
        return future.thenApplyAsync(new DeserializingFunction<>(fn), defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super V, ? extends U> fn, Executor executor) {
        return future.thenApplyAsync(new DeserializingFunction<>(fn), executor);
    }

    @Override
    public CompletableFuture<Void> thenAccept(Consumer<? super V> action) {
        return future.thenAcceptAsync(new DeserializingConsumer(action), defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super V> action) {
        return future.thenAcceptAsync(new DeserializingConsumer(action), defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super V> action, Executor executor) {
        return future.thenAcceptAsync(new DeserializingConsumer(action), executor);
    }

    @Override
    public CompletableFuture<Void> thenRun(Runnable action) {
        return future.thenRunAsync(new DeserializingRunnable(action), defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action) {
        return future.thenRunAsync(new DeserializingRunnable(action), defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
        return future.thenRunAsync(new DeserializingRunnable(action), executor);
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombine(CompletionStage<? extends U> other,
                                                     BiFunction<? super V, ? super U, ? extends V1> fn) {
        return future.thenCombineAsync(other, new DeserializingBiFunction<>(fn), defaultExecutor());
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombineAsync(CompletionStage<? extends U> other,
                                                          BiFunction<? super V, ? super U, ? extends V1> fn) {
        return future.thenCombineAsync(other, new DeserializingBiFunction<>(fn), defaultExecutor());
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombineAsync(CompletionStage<? extends U> other,
                                                          BiFunction<? super V, ? super U, ? extends V1> fn, Executor executor) {
        return future.thenCombineAsync(other, new DeserializingBiFunction<>(fn), executor);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other,
                                                      BiConsumer<? super V, ? super U> action) {
        return future.thenAcceptBothAsync(other, new DeserializingBiConsumer<>(action), defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                           BiConsumer<? super V, ? super U> action) {
        return future.thenAcceptBothAsync(other, new DeserializingBiConsumer<>(action), defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                           BiConsumer<? super V, ? super U> action, Executor executor) {
        return future.thenAcceptBothAsync(other, new DeserializingBiConsumer<>(action), executor);
    }

    @Override
    public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return future.runAfterBothAsync(other, new DeserializingRunnable(action), defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return future.runAfterBothAsync(other, new DeserializingRunnable(action), defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return future.runAfterBothAsync(other, new DeserializingRunnable(action), executor);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return applyToEitherAsync(future, other, new DeserializingFunction(fn), defaultExecutor());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends V> other, Function<? super V, U> fn) {
         return applyToEitherAsync(future, other, new DeserializingFunction(fn), defaultExecutor());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn,
                                                       Executor executor) {
        return applyToEitherAsync(future, other, new DeserializingFunction(fn), executor);
    }

    @Override
    public CompletableFuture<Void> acceptEither(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return acceptEitherAsync(future, other, new DeserializingConsumer(action), defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return acceptEitherAsync(future, other, new DeserializingConsumer(action), defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action,
                                                     Executor executor) {
        return acceptEitherAsync(future, other, new DeserializingConsumer(action), executor);
    }

    @Override
    public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return future.runAfterEitherAsync(other, new DeserializingRunnable(action), defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return future.runAfterEitherAsync(other, new DeserializingRunnable(action), defaultExecutor());
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return future.runAfterEitherAsync(other, new DeserializingRunnable(action), executor);
    }

    @Override
    public <U> CompletableFuture<U> thenCompose(Function<? super V, ? extends CompletionStage<U>> fn) {
        return future.thenComposeAsync(new DeserializingFunction<>(fn), defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn) {
        return future.thenComposeAsync(new DeserializingFunction<>(fn), defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn, Executor executor) {
        return future.thenComposeAsync(new DeserializingFunction<>(fn), executor);
    }

    @Override
    public CompletableFuture<V> whenComplete(BiConsumer<? super V, ? super Throwable> action) {
        return future.handleAsync(new WhenCompleteAdapter(action), defaultExecutor());
    }

    @Override
    public CompletableFuture<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action) {
        return future.handleAsync(new WhenCompleteAdapter(action), defaultExecutor());
    }

    @Override
    public CompletableFuture<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action, Executor executor) {
        return future.handleAsync(new WhenCompleteAdapter(action), executor);
    }

    @Override
    public <U> CompletableFuture<U> handle(BiFunction<? super V, Throwable, ? extends U> fn) {
        return future.handleAsync(new HandleBiFunction<>(fn), defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn) {
        return future.handleAsync(new HandleBiFunction<>(fn), defaultExecutor());
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
        return future.handleAsync(new HandleBiFunction<>(fn), executor);
    }

    @Override
    public CompletableFuture<V> toCompletableFuture() {
        return this;
    }

    @Override
    public CompletableFuture<V> exceptionally(Function<Throwable, ? extends V> fn) {
        return future.handleAsync(new ExceptionallyAdapter(fn), defaultExecutor());
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
    public void obtrudeValue(V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void obtrudeException(Throwable ex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumberOfDependents() {
        return future.getNumberOfDependents();
    }

    @Override
    public String toString() {
        return "ClientDelegatingFuture{future=" + future.toString() + "}";
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<Void> acceptEitherAsync(CompletableFuture stage, CompletionStage other,
                                                             Consumer action, Executor executor) {
        return stage.acceptEitherAsync(other, action, executor);
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture applyToEitherAsync(CompletableFuture stage, CompletionStage other,
                                                        Function fn, Executor executor) {
        return stage.applyToEitherAsync(other, fn, executor);
    }

    class DeserializingFunction<R> implements Function<ClientMessage, R> {
        private final Function<? super V, ? extends R> delegate;

        DeserializingFunction(@Nonnull Function<? super V, ? extends R> delegate) {
            requireNonNull(delegate);
            this.delegate = delegate;
        }

        @Override
        public R apply(ClientMessage e) {
            return delegate.apply(resolve(e));
        }
    }

    class DeserializingConsumer implements Consumer<ClientMessage> {
        private final Consumer<? super V> delegate;

        DeserializingConsumer(@Nonnull Consumer<? super V> delegate) {
            requireNonNull(delegate);
            this.delegate = delegate;
        }

        @Override
        public void accept(ClientMessage e) {
            V resolved = resolve(e);
            delegate.accept(resolved);
        }

        @Override
        public String toString() {
            return "DeserializingConsumer{" + "delegate=" + delegate + '}';
        }
    }

    class DeserializingRunnable implements Runnable {
        private final Runnable delegate;

        DeserializingRunnable(Runnable delegate) {
            requireNonNull(delegate);
            this.delegate = delegate;
        }

        @Override
        public void run() {
            // deserialize to ensure no HazelcastSerializationException occurs
            resolve(future.join());
            delegate.run();
        }
    }

    class DeserializingBiFunction<U, R> implements BiFunction<ClientMessage, U, R> {
        private final BiFunction<? super V, U, ? extends R> delegate;

        DeserializingBiFunction(@Nonnull BiFunction<? super V, U, ? extends R> delegate) {
            requireNonNull(delegate);
            this.delegate = delegate;
        }

        @Override
        public R apply(ClientMessage t, U u) {
            V resolved = t == null ? null : resolve(t);
            return delegate.apply(resolved, (U) resolveAny(u));
        }
    }

    class DeserializingBiConsumer<U> implements BiConsumer<ClientMessage, U> {
        private final BiConsumer<? super V, U> delegate;

        DeserializingBiConsumer(@Nonnull BiConsumer<? super V, U> delegate) {
            requireNonNull(delegate);
            this.delegate = delegate;
        }

        @Override
        public void accept(ClientMessage t, U u) {
            V resolved = t == null ? null : resolve(t);
            delegate.accept(resolved, (U) resolveAny(u));
        }
    }

    class HandleBiFunction<U, R> implements BiFunction<ClientMessage, Throwable, R> {
        private final BiFunction<? super V, Throwable, R> delegate;

        HandleBiFunction(@Nonnull BiFunction<? super V, Throwable, R> delegate) {
            requireNonNull(delegate);
            this.delegate = delegate;
        }

        @Override
        public R apply(ClientMessage t, Throwable u) {
            V resolved = null;
            try {
                resolved = t == null ? null : resolve(t);
            } catch (HazelcastSerializationException exc) {
                u = exc;
            }
            return delegate.apply(resolved, u);
        }
    }

    // adapts a BiConsumer to a BiFunction for implementation
    // of whenComplete methods
    class WhenCompleteAdapter implements BiFunction<ClientMessage, Throwable, V> {
        private final BiConsumer<? super V, ? super Throwable> delegate;

        WhenCompleteAdapter(@Nonnull BiConsumer<? super V, ? super Throwable> delegate) {
            requireNonNull(delegate);
            this.delegate = delegate;
        }

        @Override
        public V apply(ClientMessage message, Throwable t) {
            V resolved = null;
            Throwable delegateException = null;
            try {
                resolved = message == null ? null : resolve(message);
            } catch (HazelcastSerializationException exc) {
                t = exc;
            }
            try {
                delegate.accept(resolved, t);
            } catch (Throwable throwable) {
                delegateException = throwable;
            }
            // implement whenComplete exception handling scheme:
            // - if original future was cancelled, throw wrapped in CompletionException
            // - if t != null, throw it
            // - if delegateException != null, throw it
            // - otherwise return resolved value
            if (t != null) {
                if (t instanceof CancellationException) {
                    throw new CompletionException(t);
                } else {
                    throw sneakyThrow(t);
                }
            } else if (delegateException != null) {
                throw sneakyThrow(delegateException);
            } else {
                return resolved;
            }
        }
    }

    // adapts a Consumer to a BiFunction for implementation of exceptionally()
    class ExceptionallyAdapter implements BiFunction<ClientMessage, Throwable, V> {
        private final Function<? super Throwable, ? extends V> delegate;

        ExceptionallyAdapter(@Nonnull Function<? super Throwable, ? extends V> delegate) {
            requireNonNull(delegate);
            this.delegate = delegate;
        }

        @Override
        public V apply(ClientMessage message, Throwable t) {
            if (t != null) {
                return delegate.apply(t);
            } else {
                V resolved;
                try {
                    resolved = message == null ? null : resolve(message);
                    return resolved;
                } catch (HazelcastSerializationException throwable) {
                    return delegate.apply(throwable);
                }
            }
        }
    }
}
