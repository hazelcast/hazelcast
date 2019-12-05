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

package com.hazelcast.client.impl;

import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;
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
    private static final Object VOID = "VOID";

    final boolean deserializeResponse;
    private final ClientMessageDecoder clientMessageDecoder;
    private final Executor userExecutor;
    private volatile Object decodedResponse = VOID;

    public ClientDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
                                  SerializationService serializationService,
                                  ClientMessageDecoder clientMessageDecoder, V defaultValue, boolean deserializeResponse) {
        super(serializationService, clientInvocationFuture, defaultValue);
        this.clientMessageDecoder = clientMessageDecoder;
        this.userExecutor = clientInvocationFuture.getInvocation().getUserExecutor();
        this.deserializeResponse = deserializeResponse;
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
        if (value instanceof ClientMessage) {
            return resolve(value);
        } else {
            return (value instanceof Data && deserializeResponse)
                    ? serializationService.toObject(value) : (V) value;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected V resolve(Object object) {
        if (result != null) {
            return (V) result;
        }

        ClientMessage clientMessage = (ClientMessage) object;
        Object decoded = decodeResponse(clientMessage);
        if (deserializeResponse) {
            return serializationService.toObject(decoded);
        }
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
        return future.thenApply(new DeserializingFunction<>(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super V, ? extends U> fn) {
        return future.thenApplyAsync(new DeserializingFunction<>(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super V, ? extends U> fn, Executor executor) {
        return future.thenApplyAsync(new DeserializingFunction<>(fn), executor);
    }

    @Override
    public CompletableFuture<Void> thenAccept(Consumer<? super V> action) {
        return future.thenAccept(new DeserializingConsumer(action));
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super V> action) {
        return future.thenAcceptAsync(new DeserializingConsumer(action), userExecutor);
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super V> action, Executor executor) {
        return future.thenAcceptAsync(new DeserializingConsumer(action), executor);
    }

    @Override
    public CompletableFuture<Void> thenRun(Runnable action) {
        return future.thenRun(action);
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action) {
        return future.thenRunAsync(action, userExecutor);
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
        return future.thenRunAsync(action, executor);
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombine(CompletionStage<? extends U> other,
                                                     BiFunction<? super V, ? super U, ? extends V1> fn) {
        return future.thenCombine(other, new DeserializingBiFunction<>(fn));
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombineAsync(CompletionStage<? extends U> other,
                                                          BiFunction<? super V, ? super U, ? extends V1> fn) {
        return future.thenCombineAsync(other, new DeserializingBiFunction<>(fn), userExecutor);
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombineAsync(CompletionStage<? extends U> other,
                                                          BiFunction<? super V, ? super U, ? extends V1> fn, Executor executor) {
        return future.thenCombineAsync(other, new DeserializingBiFunction<>(fn), executor);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other,
                                                      BiConsumer<? super V, ? super U> action) {
        return future.thenAcceptBoth(other, new DeserializingBiConsumer<>(action));
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                           BiConsumer<? super V, ? super U> action) {
        return future.thenAcceptBothAsync(other, new DeserializingBiConsumer<>(action), userExecutor);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                           BiConsumer<? super V, ? super U> action, Executor executor) {
        return future.thenAcceptBothAsync(other, new DeserializingBiConsumer<>(action), executor);
    }

    @Override
    public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return future.runAfterBoth(other, action);
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return future.runAfterBothAsync(other, action);
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return future.runAfterBothAsync(other, action, executor);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return applyToEitherAsync(future, other, new DeserializingFunction(fn));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends V> other, Function<? super V, U> fn) {
         return applyToEither(future, other, new DeserializingFunction(fn));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn,
                                                       Executor executor) {
        return applyToEitherAsync(future, other, new DeserializingFunction(fn), executor);
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture applyToEither(CompletableFuture stage, CompletionStage other, Function fn) {
        return stage.applyToEither(other, fn);
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture applyToEitherAsync(CompletableFuture stage, CompletionStage other, Function fn) {
        return stage.applyToEitherAsync(other, fn);
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture applyToEitherAsync(CompletableFuture stage, CompletionStage other,
                                                       Function fn, Executor executor) {
        return stage.applyToEitherAsync(other, fn, executor);
    }

    @Override
    public CompletableFuture<Void> acceptEither(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return acceptEither(future, other, new DeserializingConsumer(action));
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<Void> acceptEither(CompletionStage stage, CompletionStage other, Consumer action) {
        return (CompletableFuture<Void>) stage.acceptEither(other, action);
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<Void> acceptEitherAsync(CompletionStage stage, CompletionStage other, Consumer action) {
        return (CompletableFuture<Void>) stage.acceptEitherAsync(other, action);
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<Void> acceptEitherAsync(CompletionStage stage, CompletionStage other,
                                                             Consumer action, Executor executor) {
        return (CompletableFuture<Void>) stage.acceptEitherAsync(other, action, executor);
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return acceptEitherAsync(future, other, new DeserializingConsumer(action));
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action,
                                                     Executor executor) {
        return acceptEitherAsync(future, other, new DeserializingConsumer(action), executor);
    }

    @Override
    public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return future.runAfterEither(other, action);
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return future.runAfterEitherAsync(other, action, userExecutor);
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return future.runAfterEitherAsync(other, action, executor);
    }

    @Override
    public <U> CompletableFuture<U> thenCompose(Function<? super V, ? extends CompletionStage<U>> fn) {
        return future.thenCompose(new DeserializingFunction<>(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn) {
        return future.thenComposeAsync(new DeserializingFunction<>(fn), userExecutor);
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn, Executor executor) {
        return future.thenComposeAsync(new DeserializingFunction<>(fn), executor);
    }

    @Override
    public CompletableFuture<V> whenComplete(BiConsumer<? super V, ? super Throwable> action) {
        return future.handle(new WhenCompleteAdapter(action));
    }

    @Override
    public CompletableFuture<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action) {
        return future.handleAsync(new WhenCompleteAdapter(action), userExecutor);
    }

    @Override
    public CompletableFuture<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action, Executor executor) {
        return future.handleAsync(new WhenCompleteAdapter(action), executor);
    }

    @Override
    public <U> CompletableFuture<U> handle(BiFunction<? super V, Throwable, ? extends U> fn) {
        return future.handle(new DeserializingBiFunction<>(fn));
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn) {
        return future.handleAsync(new DeserializingBiFunction<>(fn), userExecutor);
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
        return future.handleAsync(new DeserializingBiFunction<>(fn), executor);
    }

    @Override
    public CompletableFuture<V> toCompletableFuture() {
        return this;
    }

    @Override
    public CompletableFuture<V> exceptionally(Function<Throwable, ? extends V> fn) {
        return future.handleAsync(new ExceptionallyAdapter(fn));
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
        return future.isCompletedExceptionally();
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
        return future.toString();
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
            V resolved = message == null ? null : resolve(message);
            Throwable delegateException = null;
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
            V resolved = message == null ? null : resolve(message);
            if (t == null) {
                return resolved;
            }
            try {
                return delegate.apply(t);
            } catch (Throwable throwable) {
                throw throwable;
            }
        }
    }
}
