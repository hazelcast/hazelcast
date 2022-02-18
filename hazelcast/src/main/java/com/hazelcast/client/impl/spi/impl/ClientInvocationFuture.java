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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.AbstractInvocationFuture;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.sequence.CallIdSequence;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.spi.impl.operationservice.impl.InvocationFuture.returnOrThrowWithGetConventions;

public class ClientInvocationFuture extends AbstractInvocationFuture<ClientMessage> {

    private final ClientMessage request;
    private final ClientInvocation invocation;
    private final CallIdSequence callIdSequence;

    public ClientInvocationFuture(ClientInvocation invocation,
                                  ClientMessage request,
                                  ILogger logger,
                                  CallIdSequence callIdSequence) {
        super(logger);
        this.request = request;
        this.invocation = invocation;
        this.callIdSequence = callIdSequence;
    }

    @Override
    protected String invocationToString() {
        return request.toString();
    }

    @Override
    protected TimeoutException newTimeoutException(long timeout, TimeUnit unit) {
        return new TimeoutException();
    }

    @Override
    protected void onInterruptDetected() {
        completeExceptionallyInternal(new InterruptedException());
    }

    @Override
    protected Object resolve(Object value) {
        if (value instanceof Throwable) {
            return new ExceptionalResult((Throwable) value);
        }
        return super.resolve(value);
    }

    @Override
    protected Exception wrapToInstanceNotActiveException(RejectedExecutionException e) {
        if (!invocation.lifecycleService.isRunning()) {
            return new HazelcastClientNotActiveException("Client is shut down", e);
        }
        return e;
    }

    @Override
    protected void onComplete() {
        super.onComplete();
        callIdSequence.complete();
    }

    @Override
    public ClientMessage resolveAndThrowIfException(Object response) throws ExecutionException, InterruptedException {
        return returnOrThrowWithGetConventions(response);
    }

    public ClientInvocation getInvocation() {
        return invocation;
    }

    @Override
    public InternalCompletableFuture<ClientMessage> exceptionally(@Nonnull Function<Throwable, ? extends ClientMessage> fn) {
        return super.exceptionally(new CallIdTrackingFunction(fn));
    }

    @Override
    public InternalCompletableFuture<Void> thenAcceptAsync(@Nonnull Consumer<? super ClientMessage> action,
                                                           @Nonnull Executor executor) {
        return super.thenAcceptAsync(new CallIdTrackingConsumer(action), executor);
    }

    @Override
    public <U> InternalCompletableFuture<U> thenApplyAsync(@Nonnull Function<? super ClientMessage, ? extends U> fn,
                                                           Executor executor) {
        return super.thenApplyAsync(new CallIdTrackingFunction(fn), executor);
    }

    @Override
    public InternalCompletableFuture<Void> thenRunAsync(@Nonnull Runnable action,
                                                        @Nonnull Executor executor) {
        return super.thenRunAsync(new CallIdTrackingRunnable(action), executor);
    }

    @Override
    public <U> InternalCompletableFuture<U> thenComposeAsync(
            @Nonnull Function<? super ClientMessage, ? extends CompletionStage<U>> fn,
            @Nonnull Executor executor) {
        return super.thenComposeAsync(new CallIdTrackingFunction(fn), executor);
    }

    @Override
    public <U, R> InternalCompletableFuture<R> thenCombineAsync(
            @Nonnull CompletionStage<? extends U> other,
            @Nonnull BiFunction<? super ClientMessage, ? super U, ? extends R> fn,
            @Nonnull Executor executor) {
        return super.thenCombineAsync(other, new CallIdTrackingBiFunction(fn), executor);
    }

    @Override
    public <U> InternalCompletableFuture<Void> thenAcceptBothAsync(@Nonnull CompletionStage<? extends U> other,
                                                                   @Nonnull BiConsumer<? super ClientMessage, ? super U> action,
                                                                   @Nonnull Executor executor) {
        return super.thenAcceptBothAsync(other, new CallIdTrackingBiConsumer(action), executor);
    }

    @Override
    public InternalCompletableFuture<Void> runAfterBothAsync(@Nonnull CompletionStage<?> other, @Nonnull Runnable action,
                                                             @Nonnull Executor executor) {
        return super.runAfterBothAsync(other, new CallIdTrackingRunnable(action), executor);
    }

    @Override
    public <U> InternalCompletableFuture<U> applyToEitherAsync(@Nonnull CompletionStage<? extends ClientMessage> other,
                                                               @Nonnull Function<? super ClientMessage, U> fn,
                                                               @Nonnull Executor executor) {
        return super.applyToEitherAsync(other, new CallIdTrackingFunction(fn), executor);
    }

    @Override
    public InternalCompletableFuture<Void> acceptEitherAsync(@Nonnull CompletionStage<? extends ClientMessage> other,
                                                             @Nonnull Consumer<? super ClientMessage> action,
                                                             @Nonnull Executor executor) {
        return super.acceptEitherAsync(other, new CallIdTrackingConsumer(action), executor);
    }

    @Override
    public InternalCompletableFuture<Void> runAfterEitherAsync(@Nonnull CompletionStage<?> other, @Nonnull Runnable action,
                                                               @Nonnull Executor executor) {
        return super.runAfterEitherAsync(other, new CallIdTrackingRunnable(action), executor);
    }

    @Override
    public InternalCompletableFuture<ClientMessage> whenCompleteAsync(
            @Nonnull BiConsumer<? super ClientMessage, ? super Throwable> action, @Nonnull Executor executor) {
        return super.whenCompleteAsync(new CallIdTrackingBiConsumer(action), executor);
    }

    @Override
    public <U> InternalCompletableFuture<U> handleAsync(@Nonnull BiFunction<? super ClientMessage, Throwable, ? extends U> fn,
                                                        @Nonnull Executor executor) {
        return super.handleAsync(new CallIdTrackingBiFunction(fn), executor);
    }

    class CallIdTrackingConsumer implements Consumer {
        private final Consumer action;

        CallIdTrackingConsumer(Consumer action) {
            this.action = action;
            callIdSequence.forceNext();
        }

        @Override
        public void accept(Object o) {
            try {
                action.accept(o);
            } finally {
                callIdSequence.complete();
            }
        }
    }

    class CallIdTrackingRunnable implements Runnable {
        private final Runnable action;

        CallIdTrackingRunnable(Runnable action) {
            this.action = action;
            callIdSequence.forceNext();
        }

        @Override
        public void run() {
            try {
                action.run();
            } finally {
                callIdSequence.complete();
            }
        }
    }

    class CallIdTrackingFunction implements Function {
        private final Function action;

        CallIdTrackingFunction(Function action) {
            this.action = action;
            callIdSequence.forceNext();
        }

        @Override
        public Object apply(Object o) {
            try {
                return action.apply(o);
            } finally {
                callIdSequence.complete();
            }
        }
    }

    class CallIdTrackingBiFunction implements BiFunction {
        private final BiFunction action;

        CallIdTrackingBiFunction(BiFunction action) {
            this.action = action;
            callIdSequence.forceNext();
        }

        @Override
        public Object apply(Object t, Object u) {
            try {
                return action.apply(t, u);
            } finally {
                callIdSequence.complete();
            }
        }
    }

    class CallIdTrackingBiConsumer implements BiConsumer {
        private final BiConsumer biConsumer;

        CallIdTrackingBiConsumer(BiConsumer biConsumer) {
            this.biConsumer = biConsumer;
            callIdSequence.forceNext();
        }

        @Override
        public void accept(Object o, Object o2) {
            try {
                biConsumer.accept(o, o2);
            } finally {
                callIdSequence.complete();
            }
        }
    }
}

