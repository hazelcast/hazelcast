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
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.spi.impl.DelegatingCompletableFuture;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * TODO:
 *
 *
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
    private final ClientInvocationFuture future;
    private final SerializationService serializationService;
    private final ClientMessageDecoder clientMessageDecoder;
    private final boolean deserializeResponse;
    private final V defaultValue;
    private final Executor userExecutor;
    private volatile Object decodedResponse = VOID;

    public ClientDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
                                  SerializationService serializationService,
                                  ClientMessageDecoder clientMessageDecoder, V defaultValue, boolean deserializeResponse) {
        super(serializationService, clientInvocationFuture, defaultValue);
        this.future = clientInvocationFuture;
        this.serializationService = serializationService;
        this.clientMessageDecoder = clientMessageDecoder;
        this.defaultValue = defaultValue;
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
    public V get() throws InterruptedException, ExecutionException {
        try {
            return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        ClientMessage response = future.get(timeout, unit);
        return (V) resolveResponse(response, deserializeResponse);
    }

    @Override
    public V join() {
        ClientMessage response = future.join();
        return resolveResponse(response, deserializeResponse);
    }

    @Override
    public V joinInternal() {
        ClientMessage response = future.joinInternal();
        return resolveResponse(response, deserializeResponse);
    }

    @SuppressWarnings("unchecked")
    private V resolveResponse(ClientMessage clientMessage, boolean deserialize) {
        if (defaultValue != null) {
            return defaultValue;
        }

        Object decodedResponse = decodeResponse(clientMessage);
        if (deserialize) {
            return serializationService.toObject(decodedResponse);
        }
        return (V) decodedResponse;
    }

    private Object resolveAny(Object o, boolean deserialize) {
        if (o instanceof ClientMessage) {
            return resolveResponse((ClientMessage) o, deserialize);
        }
        if (deserialize) {
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
        return future;
    }


    //////////
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
        CompletableFuture<V> completableFuture = InternalCompletableFuture.withExecutor(userExecutor);
        future.whenComplete(new DeserializingBiConsumer<>(action, completableFuture));
        return completableFuture;
    }

    @Override
    public CompletableFuture<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action) {
        CompletableFuture<V> completableFuture = InternalCompletableFuture.withExecutor(userExecutor);
        future.whenCompleteAsync(new DeserializingBiConsumer<>(action, completableFuture), userExecutor);
        return completableFuture;
    }

    @Override
    public CompletableFuture<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action, Executor executor) {
        CompletableFuture<V> completableFuture = InternalCompletableFuture.withExecutor(userExecutor);
        future.whenCompleteAsync(new DeserializingBiConsumer<>(action, completableFuture), executor);
        return completableFuture;
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
        CompletableFuture<V> completableFuture = InternalCompletableFuture.withExecutor(userExecutor);
        future.exceptionally(throwable -> {
            try {
                V value = fn.apply(throwable);
                completableFuture.complete(value);
            } catch (Throwable t) {
                completableFuture.completeExceptionally(t);
            }
            return null;
        });
        return completableFuture;
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

        DeserializingFunction(Function<? super V, ? extends R> delegate) {
            this.delegate = delegate;
        }

        @Override
        public R apply(ClientMessage e) {
            return delegate.apply(resolveResponse(e, deserializeResponse));
        }
    }

    class DeserializingConsumer implements Consumer<ClientMessage> {
        private final Consumer<? super V> delegate;

        DeserializingConsumer(Consumer<? super V> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void accept(ClientMessage e) {
            V resolved = resolveResponse(e, deserializeResponse);
            delegate.accept(resolved);
        }

        @Override
        public String toString() {
            return "DeserializingConsumer{" + "delegate=" + delegate + '}';
        }
    }

    class DeserializingBiFunction<U, R> implements BiFunction<ClientMessage, U, R> {
        private final BiFunction<? super V, U, ? extends R> delegate;

        DeserializingBiFunction(BiFunction<? super V, U, ? extends R> delegate) {
            this.delegate = delegate;
        }

        @Override
        public R apply(ClientMessage t, U u) {
            return delegate.apply(resolveResponse(t, deserializeResponse), (U) resolveAny(u, deserializeResponse));
        }
    }

    class DeserializingBiConsumer<U> implements BiConsumer<ClientMessage, U> {
        private final BiConsumer<? super V, U> delegate;
        private final CompletableFuture<V> future;

        DeserializingBiConsumer(BiConsumer<? super V, U> delegate) {
            this.delegate = delegate;
            this.future = null;
        }

        DeserializingBiConsumer(BiConsumer<? super V, U> delegate, CompletableFuture<V> future) {
            this.delegate = delegate;
            this.future = future;
        }

        @Override
        public void accept(ClientMessage t, U u) {
            V resolved = t == null ? null : resolveResponse(t, deserializeResponse);
            try {
                delegate.accept(resolved, (U) resolveAny(u, deserializeResponse));
                if (future != null) {
                    future.complete(resolved);
                }
            } catch (Throwable throwable) {
                if (future != null) {
                    future.completeExceptionally(throwable);
                }
            }
        }
    }
}
