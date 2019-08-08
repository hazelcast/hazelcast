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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.util.executor.UnblockableThread;
import com.hazelcast.logging.ILogger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.ExceptionUtil.rethrowIfError;
import static com.hazelcast.internal.util.Preconditions.isNotNull;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import static java.util.concurrent.locks.LockSupport.park;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static java.util.concurrent.locks.LockSupport.unpark;

/**
 * Custom implementation of {@link java.util.concurrent.CompletableFuture}.
 * <p>
 * The long term goal is that this whole class can be ripped out and replaced
 * by {@link java.util.concurrent.CompletableFuture} from the JDK. So we need
 * to be very careful with adding more functionality to this class because
 * the more we add, the more
 * difficult the replacement will be.
 * <p>
 * TODO:
 * - thread value protection
 *
 * @param <V>
 */
@SuppressFBWarnings(value = "DLS_DEAD_STORE_OF_CLASS_LITERAL", justification = "Recommended way to prevent classloading bug")
public abstract class AbstractInvocationFuture<V> extends CompletableFuture<V> implements InternalCompletableFuture<V> {

    static final Object UNRESOLVED = new Object();
    private static final AtomicReferenceFieldUpdater<AbstractInvocationFuture, Object> STATE =
            newUpdater(AbstractInvocationFuture.class, Object.class, "state");

    // Default executor for async callbacks: ForkJoinPool.commonPool() or a thread-per-task executor when
    // the common pool does not support parallelism
    private static final Executor DEFAULT_ASYNC_EXECUTOR;

    // reduce the risk of rare disastrous classloading in first call to
    // LockSupport.park: https://bugs.openjdk.java.net/browse/JDK-8074773
    static {
        @SuppressWarnings("unused")
        Class<?> ensureLoaded = LockSupport.class;

        Executor asyncExecutor;
        if (ForkJoinPool.getCommonPoolParallelism() > 1) {
            asyncExecutor = ForkJoinPool.commonPool();
        } else {
            asyncExecutor = command -> new Thread(command).start();
        }
        DEFAULT_ASYNC_EXECUTOR = asyncExecutor;
    }

    protected final Executor defaultExecutor;
    protected final ILogger logger;

    /**
     * This field contains the state of the future. If the future is not
     * complete, the state can be:
     * <ol>
     * <li>{@link #UNRESOLVED}: no response is available.</li>
     * <li>Thread instance: no response is available and a thread has
     * blocked on completion (e.g. future.get)</li>
     * <li>{@link ExecutionCallback} instance: no response is available
     * and 1 {@link #andThen(ExecutionCallback)} was done using the default
     * executor</li>
     * <li>{@link WaitNode} or {@link Waiter} instance: in case of multiple
     * callback registrations or future.gets.</li>
     * </ol>
     * If the state is anything else, it is completed.
     * <p>
     * The reason why a single future.get or registered ExecutionCallback
     * doesn't create a WaitNode is that we don't want to cause additional
     * litter since most of our API calls are a get or a single ExecutionCallback.
     * <p>
     * The state field is replaced using a cas, so registration or setting a
     * response is an atomic operation and therefore not prone to data-races.
     * There is no need to use synchronized blocks.
     */
    protected volatile Object state = UNRESOLVED;

    protected AbstractInvocationFuture(@Nonnull ILogger logger) {
        this.defaultExecutor = DEFAULT_ASYNC_EXECUTOR;
        this.logger = logger;
    }

    // for testing only
    AbstractInvocationFuture(@Nonnull Executor defaultExecutor,
                                       @Nonnull ILogger logger) {
        this.defaultExecutor = defaultExecutor;
        this.logger = logger;
    }

    // methods to be overridden
    protected abstract String invocationToString();

    // invokes resolve(value), then handles outcome with get() exception throwing conventions
    protected abstract V resolveAndThrowIfException(Object state) throws ExecutionException, InterruptedException;

    protected abstract TimeoutException newTimeoutException(long timeout, TimeUnit unit);

    // CompletionStage API implementation
    @Override
    public <U> CompletableFuture<U> thenApply(@Nonnull Function<? super V, ? extends U> fn) {
        return unblockApply(fn, null);
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(@Nonnull Function<? super V, ? extends U> fn) {
        return unblockApply(fn, defaultExecutor);
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(@Nonnull Function<? super V, ? extends U> fn, Executor executor) {
        return unblockApply(fn, executor);
    }

    @Override
    public CompletableFuture<Void> thenAccept(@Nonnull Consumer<? super V> action) {
        return unblockAccept(action, null);
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(@Nonnull Consumer<? super V> action) {
        return unblockAccept(action, defaultExecutor);
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(@Nonnull Consumer<? super V> action, Executor executor) {
        return unblockAccept(action, executor);
    }

    @Override
    public CompletableFuture<Void> thenRun(@Nonnull Runnable action) {
        return unblockRun(action, null);
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(@Nonnull Runnable action) {
        return unblockRun(action, defaultExecutor);
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(@Nonnull Runnable action, Executor executor) {
        return unblockRun(action, executor);
    }

    @Override
    public <U> CompletableFuture<U> handle(@Nonnull BiFunction<? super V, Throwable, ? extends U> fn) {
        return unblockHandle(fn, null);
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(@Nonnull BiFunction<? super V, Throwable, ? extends U> fn) {
        return unblockHandle(fn, defaultExecutor);
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(@Nonnull BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
        return unblockHandle(fn, executor);
    }

    @Override
    public CompletableFuture<V> whenComplete(@Nonnull BiConsumer<? super V, ? super Throwable> action) {
        return unblockWhenComplete(action, null);
    }

    @Override
    public CompletableFuture<V> whenCompleteAsync(@Nonnull BiConsumer<? super V, ? super Throwable> action) {
        return unblockWhenComplete(action, defaultExecutor);
    }

    @Override
    public CompletableFuture<V> whenCompleteAsync(@Nonnull BiConsumer<? super V, ? super Throwable> action, Executor executor) {
        return unblockWhenComplete(action, executor);
    }

    @Override
    public <U> CompletableFuture<U> thenCompose(@Nonnull Function<? super V, ? extends CompletionStage<U>> fn) {
        return unblockCompose(fn, null);
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(@Nonnull Function<? super V, ? extends CompletionStage<U>> fn) {
        return unblockCompose(fn, defaultExecutor);
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(@Nonnull Function<? super V, ? extends CompletionStage<U>> fn,
                                                   Executor executor) {
        return unblockCompose(fn, executor);
    }

    @Override
    public <U, R> CompletableFuture<R> thenCombine(@Nonnull CompletionStage<? extends U> other,
                                                 @Nonnull BiFunction<? super V, ? super U, ? extends R> fn) {
        return unblockCombine(other, fn, null);
    }

    @Override
    public <U, R> CompletableFuture<R> thenCombineAsync(CompletionStage<? extends U> other,
                                                        BiFunction<? super V, ? super U, ? extends R> fn) {
        return unblockCombine(other, fn, defaultExecutor);
    }

    @Override
    public <U, R> CompletableFuture<R> thenCombineAsync(CompletionStage<? extends U> other,
                                                      BiFunction<? super V, ? super U, ? extends R> fn, Executor executor) {
        return unblockCombine(other, fn, executor);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other,
                                                      BiConsumer<? super V, ? super U> action) {
        return unblockAcceptBoth(other, action, null);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                         BiConsumer<? super V, ? super U> action) {
        return unblockAcceptBoth(other, action, defaultExecutor);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                         BiConsumer<? super V, ? super U> action, Executor executor) {
        return unblockAcceptBoth(other, action, executor);
    }

    @Override
    public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return unblockRunAfterBoth(other, action, null);
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return unblockRunAfterBoth(other, action, defaultExecutor);
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return unblockRunAfterBoth(other, action, executor);
    }

    @Override
    public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return unblockApplyToEither(other, fn, null);
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return unblockApplyToEither(other, fn, defaultExecutor);
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn,
                                                     Executor executor) {
        return unblockApplyToEither(other, fn, executor);
    }

    @Override
    public CompletableFuture<Void> acceptEither(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return unblockAcceptEither(other, action, null);
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return unblockAcceptEither(other, action, defaultExecutor);
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action,
                                                   Executor executor) {
        return unblockAcceptEither(other, action, executor);
    }

    public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return unblockRunAfterEither(other, action, null);
    }

    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return unblockRunAfterEither(other, action, defaultExecutor);
    }

    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return unblockRunAfterEither(other, action, executor);
    }

    @Override
    public CompletableFuture<V> toCompletableFuture() {
        return this;
    }

    boolean compareAndSetState(Object oldState, Object newState) {
        return STATE.compareAndSet(this, oldState, newState);
    }

    protected final Object getState() {
        return state;
    }

    @Override
    public final boolean isDone() {
        return isDone(state);
    }

    protected void onInterruptDetected() {
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return completeExceptionally(new CancellationException());
    }

    @Override
    public boolean isCancelled() {
        return isStateCancelled(state);
    }

    @Override
    public boolean isCompletedExceptionally() {
        return (state instanceof ExceptionalResult);
    }

    @Override
    public final V join() {
        try {
            // todo consider whether method ref lambda affects runtime perf / allocation rate
            return waitForResolution(this::resolveAndThrowWithJoinConvention);
        } catch (ExecutionException | InterruptedException e) {
            throw new AssertionError("Value resolution with join() conventions shouldn't throw ExecutionException or "
                    + "InterruptedException", e);
        }
    }

    /**
     * Similarly to {@link #join()}, returns the value when complete or throws an unchecked exception if
     * completed exceptionally. Unlike {@link #join()}, checked exceptions are not wrapped in {@link CompletionException};
     * rather they are wrapped in {@link com.hazelcast.core.HazelcastException}s.
     *
     * @return the result
     */
    public final V joinInternal() {
        try {
            return waitForResolution(this::resolveAndThrowIfException);
        } catch (ExecutionException | InterruptedException e) {
            throw rethrow(e);
        }
    }

    @Override
    public final V get() throws InterruptedException, ExecutionException {
        // todo consider whether method ref lambda affects runtime perf / allocation rate
        return waitForResolution(this::resolveAndThrowIfException);
    }

    @Override
    public final V get(final long timeout, final TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        Object response = registerWaiter(Thread.currentThread(), null);
        if (response != UNRESOLVED) {
            return resolveAndThrowIfException(response);
        }

        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        boolean interrupted = false;
        try {
            long timeoutNanos = unit.toNanos(timeout);
            while (timeoutNanos > 0) {
                parkNanos(timeoutNanos);
                timeoutNanos = deadlineNanos - System.nanoTime();

                if (isDone()) {
                    return resolveAndThrowIfException(state);
                } else if (Thread.interrupted()) {
                    interrupted = true;
                    onInterruptDetected();
                }
            }
        } finally {
            restoreInterrupt(interrupted);
        }

        unregisterWaiter(Thread.currentThread());
        throw newTimeoutException(timeout, unit);
    }

    // TODO override all CompletableFuture API methods
    @Override
    public V getNow(V valueIfAbsent) {
        return (state == UNRESOLVED) ? valueIfAbsent : join();
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        return completeExceptionallyInternal(ex);
    }

    @Override
    public void obtrudeValue(V value) {
        throw new UnsupportedOperationException("implement this");
    }

    @Override
    public void obtrudeException(Throwable ex) {
        throw new UnsupportedOperationException("implement this");
    }

    @Override
    public int getNumberOfDependents() {
        throw new UnsupportedOperationException("implement this");
    }

    private V waitForResolution(ValueResolver<V> resolver)
            throws InterruptedException, ExecutionException {
        Object response = registerWaiter(Thread.currentThread(), null);
        if (response != UNRESOLVED) {
            // no registration was done since a value is available.
            return resolver.resolveAndThrowIfException(response);
        }

        boolean interrupted = false;
        try {
            for (; ; ) {
                park();
                if (isDone()) {
                    return resolver.resolveAndThrowIfException(state);
                } else if (Thread.interrupted()) {
                    interrupted = true;
                    onInterruptDetected();
                }
            }
        } finally {
            restoreInterrupt(interrupted);
        }
    }

    @Override
    public void andThen(ExecutionCallback<V> callback) {
        andThen(callback, defaultExecutor);
    }

    @Override
    public void andThen(ExecutionCallback<V> callback, Executor executor) {
        isNotNull(callback, "callback");
        isNotNull(executor, "executor");

        Object response = registerWaiter(callback, executor);
        if (response != UNRESOLVED) {
            unblock(callback, executor);
        }
    }

    private void unblockAll(Object waiter, Executor executor) {
        while (waiter != null) {
            if (waiter instanceof Thread) {
                unpark((Thread) waiter);
                return;
            } else if (waiter instanceof ExecutionCallback) {
                unblock((ExecutionCallback) waiter, executor);
                return;
            } else if (waiter.getClass() == WaitNode.class) {
                WaitNode waitNode = (WaitNode) waiter;
                unblockAll(waitNode.waiter, waitNode.executor);
                waiter = waitNode.next;
            } else {
                unblockOtherNode(waiter, executor);
                return;
            }
        }
    }

    private CompletableFuture<Void> unblockAccept(@Nonnull final Consumer<? super V> consumer, Executor executor) {
        requireNonNull(consumer);
        final Object value = resolve(state);
        final CompletableFuture<Void> result = newCompletableFuture();
        if (value != UNRESOLVED) {
            if (cascadeException(value, result)) {
                return result;
            }
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    consumer.accept((V) value);
                    result.complete(null);
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                }
            });
            return result;
        } else {
            registerWaiter(new AcceptNode(result, consumer), executor);
            return result;
        }
    }

    /**
     *
     * @param waiter    the current wait node, see javadoc of {@link #state state field}
     * @param executor  the {@link Executor} on which to execute the action associated with {@code waiter}
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    protected void unblockOtherNode(Object waiter, Executor executor) {
        if (!(waiter instanceof Waiter)) {
            return;
        }
        Object value = resolve(state);
        if (waiter instanceof AcceptNode) {
            AcceptNode acceptNode = (AcceptNode) waiter;
            acceptNode.execute(executor, value);
        } else if (waiter instanceof ApplyNode) {
            ApplyNode applyNode = (ApplyNode) waiter;
            applyNode.execute(executor, value);
        } else if (waiter instanceof RunNode) {
            RunNode runNode = (RunNode) waiter;
            runNode.execute(executor, value);
        } else if (waiter instanceof AbstractInvocationFuture.WhenCompleteNode) {
            WhenCompleteNode whenCompleteNode = (WhenCompleteNode) waiter;
            Throwable t = (value instanceof ExceptionalResult) ? ((ExceptionalResult) value).cause : null;
            value = (value instanceof ExceptionalResult) ? null : value;
            whenCompleteNode.execute(executor, value, t);
        } else if (waiter instanceof HandleNode) {
            HandleNode handleNode = (HandleNode) waiter;
            Throwable t = (value instanceof ExceptionalResult) ? ((ExceptionalResult) value).cause : null;
            value = (value instanceof ExceptionalResult) ? null : value;
            handleNode.execute(executor, value, t);
        } else if (waiter instanceof ExceptionallyNode) {
            ((ExceptionallyNode) waiter).execute(value);
        } else if (waiter instanceof ComposeNode) {
            ((ComposeNode) waiter).execute(executor, value);
        } else if (waiter instanceof AbstractBiNode) {
            ((AbstractBiNode) waiter).execute(executor, value);
        } else if (waiter instanceof AbstractEitherNode) {
            ((AbstractEitherNode) waiter).execute(executor, value);
        }
    }

    protected void unblock(final ExecutionCallback<V> callback, Executor executor) {
        try {
            executor.execute(() -> {
                try {
                    Object value = resolve(state);
                    if (value instanceof ExceptionalResult) {
                        Throwable error = unwrap((ExceptionalResult) value);
                        callback.onFailure(error);
                    } else {
                        callback.onResponse((V) value);
                    }
                } catch (Throwable cause) {
                    logger.severe("Failed asynchronous execution of execution callback: " + callback
                            + "for call " + invocationToString(), cause);
                }
            });
        } catch (RejectedExecutionException e) {
            callback.onFailure(wrapToInstanceNotActiveException(e));
        }
    }

    protected abstract Exception wrapToInstanceNotActiveException(RejectedExecutionException e);

    // this method should not be needed; but there is a difference between client and server how it handles async throwables
    protected Throwable unwrap(ExceptionalResult result) {
        Throwable throwable = result.cause;
        if (throwable instanceof ExecutionException && throwable.getCause() != null) {
            return throwable.getCause();
        }
        return throwable;
    }

    protected V returnOrThrowWithJoinConventions(Object resolved) {
        if (!(resolved instanceof ExceptionalResult)) {
            return (V) resolved;
        }
        Throwable cause = ((ExceptionalResult) resolved).cause;
        rethrowIfError(cause);
        if (cause instanceof CompletionException) {
            throw (CompletionException) cause;
        }
        throw new CompletionException(cause);
    }

    /**
     * todo abstract method, overridden in both subclasses
     * @param value
     * @return  an {@link ExceptionalResult} wrapping a {@link Throwable} in case value is resolved
     *          to an exception. If the value is an instance of {@link com.hazelcast.nio.serialization.Data}
     *          it is deserialized
     */
    protected Object resolve(Object value) {
        return value;
    }

    protected V resolveAndThrowWithJoinConvention(Object state) {
        Object value = resolve(state);
        return returnOrThrowWithJoinConventions(value);
    }

    protected <U> CompletableFuture<U> unblockApply(@Nonnull final Function<? super V, ? extends U> function, Executor executor) {
        requireNonNull(function);
        final Object value = resolve(state);
        final CompletableFuture<U> result = newCompletableFuture();
        if (value != UNRESOLVED) {
            if (cascadeException(value, result)) {
                return result;
            }
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                result.complete(function.apply((V) value));
            });
            return result;
        } else {
            registerWaiter(new ApplyNode(result, function), executor);
            return result;
        }
    }

    protected CompletableFuture<Void> unblockRun(@Nonnull final Runnable runnable, Executor executor) {
        requireNonNull(runnable);
        final Object value = resolve(state);
        final CompletableFuture<Void> result = newCompletableFuture();
        if (value != UNRESOLVED) {
            if (cascadeException(value, result)) {
                return result;
            }
            return runAfter0(result, runnable, executor);
        } else {
            registerWaiter(new RunNode(result, runnable), executor);
            return result;
        }
    }

    private <U> CompletableFuture<U> unblockHandle(@Nonnull BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
        requireNonNull(fn);
        Object resolved = resolve(state);
        final CompletableFuture<U> future = newCompletableFuture();
        if (resolved != UNRESOLVED) {
            V value;
            Throwable throwable;
            if (resolved instanceof ExceptionalResult) {
                throwable = ((ExceptionalResult) resolved).cause;
                value = null;
            } else {
                throwable = null;
                value = (V) resolved;
            }

            if (executor != null) {
                executor.execute(() -> {
                    try {
                        U result = fn.apply(value, throwable);
                        future.complete(result);
                    } catch (Throwable t) {
                        future.completeExceptionally(t);
                    }
                });
            } else {
                try {
                    U result = fn.apply(value, throwable);
                    future.complete(result);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            }
            return future;
        } else {
            registerWaiter(new HandleNode(future, fn), executor);
            return future;
        }
    }

    protected CompletableFuture<V> unblockWhenComplete(@Nonnull final BiConsumer<? super V, ? super Throwable> biConsumer,
                                                       Executor executor) {
        requireNonNull(biConsumer);
        Object result = resolve(state);
        final CompletableFuture<V> future = newCompletableFuture();
        if (result != UNRESOLVED && isDone()) {
            V value;
            Throwable throwable;
            if (result instanceof ExceptionalResult) {
                throwable = ((ExceptionalResult) result).cause;
                value = null;
            } else {
                throwable = null;
                value = (V) result;
            }
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    biConsumer.accept((V) value, throwable);
                } catch (Throwable t) {
                    completeExceptionallyWithPriority(future, throwable, t);
                    return;
                }
                completeFuture(future, value, throwable);
            });
            return future;
        } else {
            registerWaiter(new WhenCompleteNode(future, biConsumer), executor);
            return future;
        }
    }

    @Override
    public CompletableFuture<V> exceptionally(@Nonnull Function<Throwable, ? extends V> fn) {
        requireNonNull(fn);
        Object result = resolve(state);
        final CompletableFuture<V> future = newCompletableFuture();
        if (result != UNRESOLVED && isDone()) {
            if (result instanceof ExceptionalResult) {
                Throwable throwable = ((ExceptionalResult) result).cause;
                try {
                    V value = fn.apply(throwable);
                    future.complete(value);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            } else {
                future.complete((V) result);
            }
            return future;
        } else {
            registerWaiter(new ExceptionallyNode<V>(future, fn), null);
            return future;
        }
    }

    protected <U> CompletableFuture<U> unblockCompose(@Nonnull final Function<? super V, ? extends CompletionStage<U>> function,
                                                      Executor executor) {
        requireNonNull(function);
        final Object value = resolve(state);

        CompletableFuture<U> result = newCompletableFuture();
        if (value != UNRESOLVED && isDone()) {
            if (cascadeException(value, result)) {
                return result;
            }
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    CompletionStage<U> r = function.apply((V) value);
                    r.whenComplete((v, t) -> {
                        if (t == null) {
                            result.complete(v);
                        } else {
                            result.completeExceptionally(t);
                        }
                    });
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                }
            });
            return result;
        } else {
            registerWaiter(new ComposeNode<>(result, function), executor);
            return result;
        }
    }

    protected <U, R> CompletableFuture<R> unblockCombine(@Nonnull CompletionStage<? extends U> other,
                                                         @Nonnull final BiFunction<? super V, ? super U, ? extends R> function,
                                                         Executor executor) {
        requireNonNull(other);
        requireNonNull(function);
        final Object value = resolve(state);
        final CompletableFuture<? extends U> otherFuture =
                (other instanceof CompletableFuture) ? (CompletableFuture<? extends U>) other : other.toCompletableFuture();

        CompletableFuture<R> result = newCompletableFuture();
        if (value != UNRESOLVED && isDone()) {
            // TODO does this violate contract of exception handling in CompletionStage?
            //  thenCombine would wait for both futures to complete normally, but does not
            //  indicate that it is required to wait for both when completed exceptionally
            // in case this future is completed exceptionally, the result is also exceptionally completed
            // without checking whether otherFuture is completed or not
            if (cascadeException(value, result)) {
                return result;
            }
            if (!otherFuture.isDone()) {
                // register on other future as waiter and return
                otherFuture.whenCompleteAsync((v, t) -> {
                    if (t != null) {
                        result.completeExceptionally(t);
                    }
                    try {
                        R r = function.apply((V) value, v);
                        result.complete(r);
                    } catch (Throwable e) {
                        result.completeExceptionally(e);
                    }
                }, executor);
                return result;
            }
            // both futures are done
            if (otherFuture.isCompletedExceptionally()) {
                otherFuture.exceptionally(t -> {
                    result.completeExceptionally(t);
                    return null;
                });
                return result;
            }
            U otherValue = otherFuture.join();
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    R r = function.apply((V) value, otherValue);
                    result.complete(r);
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                }
            });
            return result;
        } else {
            CombineNode waiter = new CombineNode(result, otherFuture, function);
            registerWaiter(waiter, executor);
            return result;
        }
    }

    protected <U> CompletableFuture<Void> unblockAcceptBoth(@Nonnull CompletionStage<? extends U> other,
                                                            @Nonnull final BiConsumer<? super V, ? super U> action,
                                                            Executor executor) {
        requireNonNull(other);
        requireNonNull(action);
        final Object value = resolve(state);
        final CompletableFuture<? extends U> otherFuture =
                (other instanceof CompletableFuture) ? (CompletableFuture<? extends U>) other : other.toCompletableFuture();

        CompletableFuture<Void> result = newCompletableFuture();
        if (value != UNRESOLVED && isDone()) {
            // TODO does this violate contract of exception handling in CompletionStage?
            //  thenCombine would wait for both futures to complete normally, but does not
            //  indicate that it is required to wait for both when completed exceptionally
            // in case this future is completed exceptionally, the result is also exceptionally completed
            // without checking whether otherFuture is completed or not
            if (cascadeException(value, result)) {
                return result;
            }
            if (!otherFuture.isDone()) {
                // register on other future as waiter and return
                otherFuture.whenCompleteAsync((u, t) -> {
                    if (t != null) {
                        result.completeExceptionally(t);
                    }
                    try {
                        action.accept((V) value, u);
                        result.complete(null);
                    } catch (Throwable e) {
                        result.completeExceptionally(e);
                    }
                }, executor);
                return result;
            }
            // both futures are done
            if (otherFuture.isCompletedExceptionally()) {
                otherFuture.exceptionally(t -> {
                    result.completeExceptionally(t);
                    return null;
                });
                return result;
            }
            U otherValue = otherFuture.join();
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    action.accept((V) value, otherValue);
                    result.complete(null);
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                }
            });
            return result;
        } else {
            AcceptBothNode waiter = new AcceptBothNode(result, otherFuture, action);
            registerWaiter(waiter, executor);
            return result;
        }
    }

    protected <U> CompletableFuture<Void> unblockRunAfterBoth(@Nonnull CompletionStage<? extends U> other,
                                                              @Nonnull final Runnable action,
                                                              Executor executor) {
        requireNonNull(other);
        requireNonNull(action);
        final Object value = resolve(state);
        final CompletableFuture<? extends U> otherFuture =
                (other instanceof CompletableFuture) ? (CompletableFuture<? extends U>) other : other.toCompletableFuture();

        CompletableFuture<Void> result = newCompletableFuture();
        if (value != UNRESOLVED && isDone()) {
            // TODO does this violate contract of exception handling in CompletionStage?
            //  thenCombine would wait for both futures to complete normally, but does not
            //  indicate that it is required to wait for both when completed exceptionally
            // in case this future is completed exceptionally, the result is also exceptionally completed
            // without checking whether otherFuture is completed or not
            if (cascadeException(value, result)) {
                return result;
            }
            if (!otherFuture.isDone()) {
                // register on other future as waiter and return
                otherFuture.whenCompleteAsync((u, t) -> {
                    if (t != null) {
                        result.completeExceptionally(t);
                    }
                    try {
                        action.run();
                        result.complete(null);
                    } catch (Throwable e) {
                        result.completeExceptionally(e);
                    }
                }, executor);
                return result;
            }
            // both futures are done
            if (otherFuture.isCompletedExceptionally()) {
                otherFuture.exceptionally(t -> {
                    result.completeExceptionally(t);
                    return null;
                });
                return result;
            }
            return runAfter0(result, action, executor);
        } else {
            RunAfterBothNode waiter = new RunAfterBothNode(result, otherFuture, action);
            registerWaiter(waiter, executor);
            return result;
        }
    }

    protected <U> CompletableFuture<U> unblockApplyToEither(@Nonnull CompletionStage other,
                                                            @Nonnull final Function<? super V, U> action,
                                                            Executor executor) {
        requireNonNull(other);
        requireNonNull(action);
        final Object value = resolve(state);
        final CompletableFuture<? extends V> otherFuture =
                (other instanceof CompletableFuture) ? (CompletableFuture<? extends V>) other : other.toCompletableFuture();

        CompletableFuture<U> result = newCompletableFuture();
        if (value != UNRESOLVED && isDone()) {
            // in case this future is completed exceptionally, the result is also exceptionally completed
            if (cascadeException(value, result)) {
                return result;
            }
            return applyTo0(result, action, executor, (V) value);
        } else if (otherFuture.isDone()) {
            if (otherFuture.isCompletedExceptionally()) {
                otherFuture.whenComplete((v, t) -> {
                    result.completeExceptionally(t);
                });
                return result;
            }
            return applyTo0(result, action, executor, (V) otherFuture.join());
        } else {
            ApplyEither waiter = new ApplyEither(result, action);
            registerWaiter(waiter, executor);
            otherFuture.whenCompleteAsync(waiter, executor);
            return result;
        }
    }

    protected CompletableFuture<Void> unblockAcceptEither(@Nonnull CompletionStage other,
                                                          @Nonnull final Consumer<? super V> action,
                                                          Executor executor) {
        requireNonNull(other);
        requireNonNull(action);
        final Object value = resolve(state);
        final CompletableFuture<? extends V> otherFuture =
                (other instanceof CompletableFuture) ? (CompletableFuture<? extends V>) other : other.toCompletableFuture();

        CompletableFuture<Void> result = newCompletableFuture();
        if (value != UNRESOLVED && isDone()) {
            // in case this future is completed exceptionally, the result is also exceptionally completed
            if (cascadeException(value, result)) {
                return result;
            }
            return acceptAfter0(result, action, executor, (V) value);
        } else if (otherFuture.isDone()) {
            if (otherFuture.isCompletedExceptionally()) {
                otherFuture.whenComplete((v, t) -> {
                    result.completeExceptionally(t);
                });
                return result;
            }
            return acceptAfter0(result, action, executor, (V) otherFuture.join());
        } else {
            AcceptEither waiter = new AcceptEither(result, action);
            registerWaiter(waiter, executor);
            otherFuture.whenCompleteAsync(waiter, executor);
            return result;
        }
    }

    private CompletableFuture<Void> unblockRunAfterEither(@Nonnull CompletionStage other,
                                                          @Nonnull final Runnable action,
                                                          Executor executor) {
        requireNonNull(other);
        requireNonNull(action);
        final Object value = resolve(state);
        final CompletableFuture<? extends V> otherFuture =
                (other instanceof CompletableFuture) ? (CompletableFuture<? extends V>) other : other.toCompletableFuture();

        CompletableFuture<Void> result = newCompletableFuture();
        if (value != UNRESOLVED && isDone()) {
            // in case this future is completed exceptionally, the result is also exceptionally completed
            if (cascadeException(value, result)) {
                return result;
            }
            return runAfter0(result, action, executor);
        } else if (otherFuture.isDone()) {
            if (otherFuture.isCompletedExceptionally()) {
                otherFuture.whenComplete((v, t) -> {
                    result.completeExceptionally(t);
                });
                return result;
            }
            return runAfter0(result, action, executor);
        } else {
            RunAfterEither waiter = new RunAfterEither(result, action);
            registerWaiter(waiter, executor);
            otherFuture.whenCompleteAsync(waiter, executor);
            return result;
        }
    }

    /**
     * Registers a waiter (thread/ExecutionCallback) that gets notified when
     * the future completes.
     *
     * @param waiter   the waiter
     * @param executor the {@link Executor} to use in case of an
     *                 {@link ExecutionCallback}.
     * @return UNRESOLVED if the registration was a success, anything else but void
     * is the response.
     */
    private Object registerWaiter(Object waiter, Executor executor) {
        assert !(waiter instanceof UnblockableThread) : "Waiting for response on this thread is illegal";
        WaitNode waitNode = null;
        for (; ; ) {
            final Object oldState = state;
            if (isDone(oldState)) {
                return oldState;
            }

            Object newState;
            if (oldState == UNRESOLVED && (executor == null || executor == defaultExecutor)) {
                // nothing is syncing on this future, so instead of creating a WaitNode, we just try to cas the waiter
                newState = waiter;
            } else {
                // something already has been registered for syncing, so we need to create a WaitNode
                if (waitNode == null) {
                    waitNode = new WaitNode(waiter, executor);
                }
                waitNode.next = oldState;
                newState = waitNode;
            }

            if (compareAndSetState(oldState, newState)) {
                // we have successfully registered
                return UNRESOLVED;
            }
        }
    }

    void unregisterWaiter(Thread waiter) {
        WaitNode prev = null;
        Object current = state;

        while (current != null) {
            Object currentWaiter = current.getClass() == WaitNode.class ? ((WaitNode) current).waiter : current;
            Object next = current.getClass() == WaitNode.class ? ((WaitNode) current).next : null;

            if (currentWaiter == waiter) {
                // it is the item we are looking for, so lets try to remove it
                if (prev == null) {
                    // it's the first item of the stack, so we need to change the head to the next
                    Object n = next == null ? UNRESOLVED : next;
                    // if we manage to CAS we are done, else we need to restart
                    current = compareAndSetState(current, n) ? null : state;
                } else {
                    // remove the current item (this is done by letting the prev.next point to the next instead of current)
                    prev.next = next;
                    // end the loop
                    current = null;
                }
            } else {
                // it isn't the item we are looking for, so lets move on to the next
                prev = current.getClass() == WaitNode.class ? (WaitNode) current : null;
                current = next;
            }
        }
    }

    /**
     * Can be called multiple times, but only the first answer will lead to the
     * future getting triggered. All subsequent complete calls are ignored.
     *
     * @param value The type of response to offer.
     * @return <tt>true</tt> if offered response, either a final response or an
     * internal response, is set/applied, <tt>false</tt> otherwise. If <tt>false</tt>
     * is returned, that means offered response is ignored because a final response
     * is already set to this future.
     */
    @Override
    public final boolean complete(Object value) {
        return complete0(value);
    }

    public final boolean completeExceptionallyInternal(Object value) {
        return complete0(wrapThrowable(value));
    }

    private boolean complete0(Object value) {
        for (; ; ) {
            final Object oldState = state;
            if (isDone(oldState)) {
                warnIfSuspiciousDoubleCompletion(oldState, value);
                return false;
            }
            if (compareAndSetState(oldState, value)) {
                onComplete();
                unblockAll(oldState, defaultExecutor);
                return true;
            }
        }
    }

    protected void onComplete() {

    }

    // it can be that this future is already completed, e.g. when an invocation already
    // received a response, but before it cleans up itself, it receives a HazelcastInstanceNotActiveException
    private void warnIfSuspiciousDoubleCompletion(Object s0, Object s1) {
        if (s0 != s1 && !(isStateCancelled(s0)) && !(isStateCancelled(s1))) {
            logger.warning(String.format("Future.complete(Object) on completed future. "
                            + "Request: %s, current value: %s, offered value: %s",
                    invocationToString(), s0, s1), new Exception());
        }
    }

    @Override
    public String toString() {
        Object state = getState();
        if (isDone(state)) {
            return "InvocationFuture{invocation=" + invocationToString() + ", value=" + state + '}';
        } else {
            return "InvocationFuture{invocation=" + invocationToString() + ", done=false}";
        }
    }

    private CompletableFuture<Void> runAfter0(CompletableFuture<Void> result, @Nonnull Runnable action, Executor executor) {
        Executor e = (executor == null) ? CALLER_RUNS : executor;
        e.execute(() -> {
            try {
                action.run();
                result.complete(null);
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        return result;
    }

    private CompletableFuture<Void> acceptAfter0(CompletableFuture<Void> result, @Nonnull Consumer<? super V> consumer,
                                               Executor executor, V value) {
        Executor e = (executor == null) ? CALLER_RUNS : executor;
        e.execute(() -> {
            try {
                consumer.accept(value);
                result.complete(null);
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        return result;
    }

    private <U> CompletableFuture<U> applyTo0(@Nonnull CompletableFuture<U> result,
                                            @Nonnull Function<? super V, U> consumer,
                                            Executor executor, V value) {
        Executor e = (executor == null) ? CALLER_RUNS : executor;
        e.execute(() -> {
            try {
                result.complete(consumer.apply(value));
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        return result;
    }

    <T> CompletableFuture<T> newCompletableFuture() {
        // todo
        return new CompletableFuture<T>();
    }

    /**
     * If {@code resolved} is an {@link ExceptionalResult}, complete the {@code dependent}
     * exceptionally with a {@link CompletionException} that wraps the cause.
     * Used as discussed in {@link CompletionStage} javadoc regarding exceptional completion
     * of dependents.
     *
     * @param resolved  a resolved state, as returned from {@link #resolve(Object)}
     * @param dependent a dependent {@link CompletableFuture}
     * @return          {@code true} in case the dependent was completed exceptionally, otherwise {@code false}
     */
    public static boolean cascadeException(Object resolved, CompletableFuture dependent) {
        if (resolved instanceof ExceptionalResult) {
            dependent.completeExceptionally(wrapInCompletionException((((ExceptionalResult) resolved).cause)));
            return true;
        }
        return false;
    }

    public static CompletionException wrapInCompletionException(Throwable t) {
        return (t instanceof CompletionException)
                ? (CompletionException) t
                : new CompletionException(t);
    }

    private ExceptionalResult wrapThrowable(Object value) {
        if (value instanceof ExceptionalResult) {
            return (ExceptionalResult) value;
        }
        return new ExceptionalResult((Throwable) value);
    }

    /**
     * Linked nodes to record waiting {@link Thread} or {@link ExecutionCallback}
     * instances using a Treiber stack.
     * <p>
     * A waiter is something that gets triggered when a response comes in. There
     * are 2 types of waiters:
     * <ol>
     * <li>Thread: when a future.get is done.</li>
     * <li>ExecutionCallback: when a future.andThen is done</li>
     * </ol>
     * The waiter is either a Thread or an ExecutionCallback.
     * <p>
     * The {@link WaitNode} is effectively immutable. Once the WaitNode is set in
     * the 'state' field, it will not be modified. Also updating the state,
     * introduces a happens before relation so the 'next' field can be read safely.
     */
    static final class WaitNode {
        final Object waiter;
        volatile Object next;
        private final Executor executor;

        WaitNode(Object waiter, Executor executor) {
            this.waiter = waiter;
            this.executor = executor;
        }
    }

    // todo this should be an implementation detail but is currently reused in Invocation.pendingResponse
    public static final class ExceptionalResult {
        private final Throwable cause;

        public ExceptionalResult(Throwable cause) {
            this.cause = cause;
        }

        public Throwable getCause() {
            return cause;
        }

        @Override
        public String toString() {
            return "ExceptionalResult{" + "cause=" + cause + '}';
        }
    }

    interface Waiter {
        // marker interface for waiter nodes
    }

    // a WaitNode for a Function<V, R>
    protected static final class ApplyNode<V, R> implements Waiter {
        final CompletableFuture<R> future;
        final Function<V, R> function;

        public ApplyNode(CompletableFuture<R> future, Function<V, R> function) {
            this.future = future;
            this.function = function;
        }

        public void execute(Executor executor, V value) {
            if (cascadeException(value, future)) {
                return;
            }
            if (executor == null) {
                future.complete(function.apply(value));
            } else {
                executor.execute(() -> {
                    future.complete(function.apply(value));
                });
            }
        }
    }

    // a WaitNode for exceptionally(Function<Throwable, V>)
    protected static final class ExceptionallyNode<R> implements Waiter {
        final CompletableFuture<R> future;
        final Function<Throwable, ? extends R> function;

        public ExceptionallyNode(CompletableFuture<R> future, Function<Throwable, ? extends R> function) {
            this.future = future;
            this.function = function;
        }

        public void execute(Object resolved) {
            if (resolved instanceof ExceptionalResult) {
                Throwable throwable = ((ExceptionalResult) resolved).cause;
                try {
                    R value = function.apply(throwable);
                    future.complete(value);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            } else {
                future.complete((R) resolved);
            }
        }
    }

    // a WaitNode for a BiFunction<V, T, R>
    protected static final class HandleNode<V, T, R> implements Waiter {
        final CompletableFuture<R> future;
        final BiFunction<V, T, R> biFunction;

        public HandleNode(CompletableFuture<R> future, BiFunction<V, T, R> biFunction) {
            this.future = future;
            this.biFunction = biFunction;
        }

        public void execute(Executor executor, V value, T throwable) {
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    future.complete(biFunction.apply(value, throwable));
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        }
    }

    // a WaitNode for a BiConsumer<V, T>
    protected static final class WhenCompleteNode<V, T extends Throwable> implements Waiter {
        final CompletableFuture<V> future;
        final BiConsumer<V, T> biFunction;

        public WhenCompleteNode(@Nonnull CompletableFuture<V> future, @Nonnull BiConsumer<V, T> biFunction) {
            this.future = future;
            this.biFunction = biFunction;
        }

        public void execute(Executor executor, V value, T throwable) {
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    biFunction.accept(value, throwable);
                } catch (Throwable t) {
                    completeExceptionallyWithPriority(future, throwable, t);
                    return;
                }
                complete(value, throwable);
            });
        }

        private void complete(V value, T throwable) {
            if (throwable == null) {
                future.complete(value);
            } else {
                future.completeExceptionally(throwable);
            }
        }
    }

    // a WaitNode for a Consumer<? super V>
    protected static final class AcceptNode<T> implements Waiter {
        final CompletableFuture<Void> future;
        final Consumer<T> consumer;

        AcceptNode(@Nonnull CompletableFuture<Void> future, @Nonnull Consumer<T> consumer) {
            this.future = future;
            this.consumer = consumer;
        }

        public void execute(Executor executor, Object value) {
            if (cascadeException(value, future)) {
                return;
            }
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    consumer.accept((T) value);
                    future.complete(null);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        }
    }

    // a WaitNode for a Runnable
    protected static final class RunNode implements Waiter {
        final CompletableFuture<Void> future;
        final Runnable runnable;

        RunNode(@Nonnull CompletableFuture<Void> future, @Nonnull Runnable runnable) {
            this.future = future;
            this.runnable = runnable;
        }

        public void execute(Executor executor, Object resolved) {
            if (cascadeException(resolved, future)) {
                return;
            }
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    runnable.run();
                    future.complete(null);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        }
    }

    protected static final class ComposeNode<T, U> implements Waiter {
        final CompletableFuture<U> future;
        final Function<? super T, ? extends CompletionStage<U>> function;

        public ComposeNode(CompletableFuture<U> future, Function<? super T, ? extends CompletionStage<U>> function) {
            this.future = future;
            this.function = function;
        }

        public void execute(Executor executor, Object resolved) {
            if (cascadeException(resolved, future)) {
                return;
            }
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    CompletionStage<U> r = function.apply((T) resolved);
                    r.whenComplete((v, t) -> {
                        if (t == null) {
                            future.complete(v);
                        } else {
                            future.completeExceptionally(t);
                        }
                    });
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        }
    }

    // common superclass of waiters for two futures (combine, acceptBoth, runAfterBoth)
    protected abstract static class AbstractBiNode<T, U, R> implements Waiter {
        final CompletableFuture<R> result;
        final CompletableFuture<? extends U> otherFuture;

        public AbstractBiNode(CompletableFuture<R> future,
                           CompletableFuture<? extends U> otherFuture) {
            this.result = future;
            this.otherFuture = otherFuture;
        }

        public void execute(Executor executor, Object resolved) {
            // todo do we need additional coordination to avoid having combiner
            //  executed twice? (once by other future and another time by this??
            if (cascadeException(resolved, result)) {
                return;
            }
            if (!otherFuture.isDone()) {
                // register on other future and exit
                otherFuture.whenCompleteAsync((u, t) -> {
                    if (t != null) {
                        result.completeExceptionally(t);
                    }
                    try {
                        R r = process((T) resolved, u);
                        result.complete(r);
                    } catch (Throwable e) {
                        result.completeExceptionally(e);
                    }
                }, executor);
                return;
            }
            if (otherFuture.isCompletedExceptionally()) {
                otherFuture.exceptionally(t -> {
                    result.completeExceptionally(t);
                    return null;
                });
                return;
            }
            U otherValue = otherFuture.join();
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    R r = process((T) resolved, otherValue);
                    result.complete(r);
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                }
            });
        }

        abstract R process(T t, U u);
    }

    protected static final class CombineNode<T, U, R> extends AbstractBiNode<T, U, R> {
        final BiFunction<? super T, ? super U, ? extends R> function;

        public CombineNode(CompletableFuture<R> future,
                           CompletableFuture<? extends U> otherFuture,
                           BiFunction<? super T, ? super U, ? extends R> function) {
            super(future, otherFuture);
            this.function = function;
        }

        @Override
        R process(T t, U u) {
            return function.apply(t, u);
        }
    }

    protected static final class AcceptBothNode<T, U> extends AbstractBiNode<T, U, Void> {
        final BiConsumer<? super T, ? super U> action;

        public AcceptBothNode(CompletableFuture<Void> future,
                           CompletableFuture<? extends U> otherFuture,
                              BiConsumer<? super T, ? super U> action) {
            super(future, otherFuture);
            this.action = action;
        }

        @Override
        Void process(T t, U u) {
            action.accept(t, u);
            return null;
        }
    }

    protected static final class RunAfterBothNode<T, U> extends AbstractBiNode<T, U, Void> {
        final Runnable action;

        public RunAfterBothNode(CompletableFuture<Void> future,
                           CompletableFuture<? extends U> otherFuture,
                              Runnable action) {
            super(future, otherFuture);
            this.action = action;
        }

        @Override
        Void process(T t, U u) {
            action.run();
            return null;
        }
    }

    // common superclass of waiters for either of two futures (applyEither, acceptEither, runAfterEither)
    protected abstract static class AbstractEitherNode<T, R> implements Waiter, BiConsumer<T, Throwable> {
        final CompletableFuture<R> result;
        final AtomicBoolean executed;

        public AbstractEitherNode(CompletableFuture<R> future) {
            this.result = future;
            this.executed = new AtomicBoolean();
        }

        public void execute(Executor executor, Object resolved) {
            if (!executed.compareAndSet(false, true)) {
                return;
            }
            if (cascadeException(resolved, result)) {
                return;
            }
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    R r = process((T) resolved);
                    result.complete(r);
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                }
            });
        }

        @Override
        public void accept(T t, Throwable throwable) {
            if (!executed.compareAndSet(false, true)) {
                return;
            }
            if (throwable != null) {
                result.completeExceptionally(throwable);
            }
            try {
                R r = process(t);
                result.complete(r);
            } catch (Throwable e) {
                result.completeExceptionally(e);
            }
        }

        abstract R process(T t);
    }

    protected static final class RunAfterEither<T> extends AbstractEitherNode<T, Void> {
        final Runnable action;

        public RunAfterEither(CompletableFuture<Void> future, Runnable action) {
            super(future);
            this.action = action;
        }

        @Override
        Void process(T t) {
            action.run();
            return null;
        }
    }

    protected static final class AcceptEither<T> extends AbstractEitherNode<T, Void> {
        final Consumer<T> action;

        public AcceptEither(CompletableFuture<Void> future, Consumer<T> action) {
            super(future);
            this.action = action;
        }

        @Override
        Void process(T t) {
            action.accept(t);
            return null;
        }
    }

    protected static final class ApplyEither<T, R> extends AbstractEitherNode<T, R> {
        final Function<T, R> action;

        public ApplyEither(CompletableFuture<R> future, Function<T, R> action) {
            super(future);
            this.action = action;
        }

        @Override
        R process(T t) {
            return action.apply(t);
        }
    }

    private static boolean isStateCancelled(final Object state) {
        return ((state instanceof ExceptionalResult)
                && (((ExceptionalResult) state).cause instanceof CancellationException));
    }

    private static void completeExceptionallyWithPriority(CompletableFuture future, Throwable first,
                                                            Throwable second) {
        if (first == null) {
            future.completeExceptionally(second);
        } else {
            future.completeExceptionally(first);
        }
    }

    private static <V> void completeFuture(CompletableFuture<V> future, V value, Throwable throwable) {
        if (throwable == null) {
            future.complete(value);
        } else {
            future.completeExceptionally(throwable);
        }
    }

    private static boolean isDone(final Object state) {
        if (state == null) {
            return true;
        }

        return !(state == UNRESOLVED
                || state instanceof WaitNode
                || state instanceof Thread
                || state instanceof ExecutionCallback
                || state instanceof Waiter);
    }

    private static void restoreInterrupt(boolean interrupted) {
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    interface ValueResolver<E> {
        E resolveAndThrowIfException(Object unresolved) throws ExecutionException, InterruptedException;
    }
}
