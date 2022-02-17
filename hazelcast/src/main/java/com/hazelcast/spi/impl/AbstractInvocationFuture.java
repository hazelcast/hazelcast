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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.executor.UnblockableThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationservice.WrappableException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
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

import static com.hazelcast.internal.util.ExceptionUtil.cloneExceptionWithFixedAsyncStackTrace;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import static java.util.concurrent.locks.LockSupport.park;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static java.util.concurrent.locks.LockSupport.unpark;

/**
 * Custom implementation of {@link java.util.concurrent.CompletableFuture}.
 *
 * @param <V>
 */
@SuppressFBWarnings(value = "DLS_DEAD_STORE_OF_CLASS_LITERAL", justification = "Recommended way to prevent classloading bug")
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public abstract class AbstractInvocationFuture<V> extends InternalCompletableFuture<V> {

    static final Object UNRESOLVED = new Object() {
        @Override
        public String toString() {
            return "UNRESOLVED";
        }
    };

    private static final AtomicReferenceFieldUpdater<AbstractInvocationFuture, Object> STATE_UPDATER =
            newUpdater(AbstractInvocationFuture.class, Object.class, "state");

    // reduce the risk of rare disastrous classloading in first call to
    // LockSupport.park: https://bugs.openjdk.java.net/browse/JDK-8074773
    static {
        @SuppressWarnings("unused")
        Class<?> ensureLoaded = LockSupport.class;
    }

    protected final ILogger logger;

    /**
     * This field contains the state of the future. If the future is not
     * complete, the state can be:
     * <ol>
     * <li>{@link #UNRESOLVED}: no response is available.</li>
     * <li>Thread instance: no response is available and a thread has
     * blocked on completion (e.g. future.get)</li>
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
        this.logger = logger;
    }

    // methods to be overridden
    protected abstract String invocationToString();

    // invokes resolve(value), then handles outcome with get() exception throwing conventions
    protected abstract V resolveAndThrowIfException(Object state) throws ExecutionException, InterruptedException;

    protected abstract TimeoutException newTimeoutException(long timeout, TimeUnit unit);

    // CompletionStage API implementation
    @Override
    public <U> InternalCompletableFuture<U> thenApply(@Nonnull Function<? super V, ? extends U> fn) {
        return thenApplyAsync(fn, defaultExecutor());
    }

    @Override
    public <U> InternalCompletableFuture<U> thenApplyAsync(@Nonnull Function<? super V, ? extends U> fn) {
        return thenApplyAsync(fn, defaultExecutor());
    }

    @Override
    public <U> InternalCompletableFuture<U> thenApplyAsync(@Nonnull Function<? super V, ? extends U> fn, Executor executor) {
        requireNonNull(fn);
        requireNonNull(executor);
        final InternalCompletableFuture<U> future = newCompletableFuture();
        if (isDone()) {
            unblockApply(fn, executor, future);
        } else {
            Object result = registerWaiter(new ApplyNode(future, fn), executor);
            if (result != UNRESOLVED) {
                unblockApply(fn, executor, future);
            }
        }
        return future;
    }

    @Override
    public InternalCompletableFuture<Void> thenAccept(@Nonnull Consumer<? super V> action) {
        return thenAcceptAsync(action, defaultExecutor());
    }

    @Override
    public InternalCompletableFuture<Void> thenAcceptAsync(@Nonnull Consumer<? super V> action) {
        return thenAcceptAsync(action, defaultExecutor());
    }

    @Override
    public InternalCompletableFuture<Void> thenAcceptAsync(@Nonnull Consumer<? super V> action,
                                                           @Nonnull Executor executor) {
        requireNonNull(action);
        requireNonNull(executor);
        final InternalCompletableFuture<Void> future = newCompletableFuture();
        if (isDone()) {
            unblockAccept(action, executor, future);
        } else {
            Object result = registerWaiter(new AcceptNode<>(future, action), executor);
            if (result != UNRESOLVED) {
                unblockAccept(action, executor, future);
            }
        }
        return future;
    }

    @Override
    public InternalCompletableFuture<Void> thenRun(@Nonnull Runnable action) {
        return thenRunAsync(action, defaultExecutor());
    }

    @Override
    public InternalCompletableFuture<Void> thenRunAsync(@Nonnull Runnable action) {
        return thenRunAsync(action, defaultExecutor());
    }

    @Override
    public InternalCompletableFuture<Void> thenRunAsync(@Nonnull Runnable action, @Nonnull Executor executor) {
        requireNonNull(action);
        requireNonNull(executor);
        final InternalCompletableFuture<Void> future = newCompletableFuture();
        if (isDone()) {
            unblockRun(action, executor, future);
        } else {
            Object result = registerWaiter(new RunNode(future, action), executor);
            if (result != UNRESOLVED) {
                unblockRun(action, executor, future);
            }
        }
        return future;
    }

    @Override
    public <U> InternalCompletableFuture<U> handle(@Nonnull BiFunction<? super V, Throwable, ? extends U> fn) {
        return handleAsync(fn, defaultExecutor());
    }

    @Override
    public <U> InternalCompletableFuture<U> handleAsync(@Nonnull BiFunction<? super V, Throwable, ? extends U> fn) {
        return handleAsync(fn, defaultExecutor());
    }

    @Override
    public <U> InternalCompletableFuture<U> handleAsync(@Nonnull BiFunction<? super V, Throwable, ? extends U> fn,
                                                        @Nonnull Executor executor) {
        requireNonNull(fn);
        requireNonNull(executor);
        final InternalCompletableFuture<U> future = newCompletableFuture();
        if (isDone()) {
            unblockHandle(fn, executor, future);
        } else {
            Object result = registerWaiter(new HandleNode(future, fn), executor);
            if (result != UNRESOLVED) {
                unblockHandle(fn, executor, future);
            }
        }
        return future;
    }

    @Override
    public InternalCompletableFuture<V> whenComplete(@Nonnull BiConsumer<? super V, ? super Throwable> action) {
        return whenCompleteAsync(action, defaultExecutor());
    }

    @Override
    public InternalCompletableFuture<V> whenCompleteAsync(@Nonnull BiConsumer<? super V, ? super Throwable> action) {
        return whenCompleteAsync(action, defaultExecutor());
    }

    @Override
    public InternalCompletableFuture<V> whenCompleteAsync(@Nonnull BiConsumer<? super V, ? super Throwable> action,
                                                          @Nonnull Executor executor) {
        requireNonNull(action);
        requireNonNull(executor);
        final InternalCompletableFuture<V> future = newCompletableFuture();
        if (isDone()) {
            unblockWhenComplete(action, executor, future);
        } else {
            Object result = registerWaiter(new WhenCompleteNode(future, action), executor);
            if (result != UNRESOLVED) {
                unblockWhenComplete(action, executor, future);
            }
        }
        return future;
    }

    @Override
    public <U> InternalCompletableFuture<U> thenCompose(@Nonnull Function<? super V, ? extends CompletionStage<U>> fn) {
        return thenComposeAsync(fn, defaultExecutor());
    }

    @Override
    public <U> InternalCompletableFuture<U> thenComposeAsync(@Nonnull Function<? super V, ? extends CompletionStage<U>> fn) {
        return thenComposeAsync(fn, defaultExecutor());
    }

    @Override
    public <U> InternalCompletableFuture<U> thenComposeAsync(@Nonnull Function<? super V, ? extends CompletionStage<U>> fn,
                                                             @Nonnull Executor executor) {
        requireNonNull(fn);
        requireNonNull(executor);
        final InternalCompletableFuture<U> future = newCompletableFuture();
        if (isDone()) {
            unblockCompose(fn, executor, future);
        } else {
            Object result = registerWaiter(new ComposeNode<V, U>(future, fn), executor);
            if (result != UNRESOLVED) {
                unblockCompose(fn, executor, future);
            }
        }
        return future;
    }

    @Override
    public <U, R> InternalCompletableFuture<R> thenCombine(@Nonnull CompletionStage<? extends U> other,
                                                           @Nonnull BiFunction<? super V, ? super U, ? extends R> fn) {
        return thenCombineAsync(other, fn, defaultExecutor());
    }

    @Override
    public <U, R> InternalCompletableFuture<R> thenCombineAsync(@Nonnull CompletionStage<? extends U> other,
                                                                @Nonnull BiFunction<? super V, ? super U, ? extends R> fn) {
        return thenCombineAsync(other, fn, defaultExecutor());
    }

    @Override
    public <U, R> InternalCompletableFuture<R> thenCombineAsync(@Nonnull CompletionStage<? extends U> other,
                                                                @Nonnull BiFunction<? super V, ? super U, ? extends R> fn,
                                                                @Nonnull Executor executor) {
        requireNonNull(other);
        requireNonNull(fn);
        requireNonNull(executor);
        final InternalCompletableFuture<R> future = newCompletableFuture();
        if (isDone()) {
            unblockCombine(other, fn, executor, future);
        } else {
            Object result = registerWaiter(new CombineNode<V, U, R>(future, other.toCompletableFuture(), fn), executor);
            if (result != UNRESOLVED) {
                unblockCombine(other, fn, executor, future);
            }
        }
        return future;
    }

    @Override
    public <U> InternalCompletableFuture<Void> thenAcceptBoth(@Nonnull CompletionStage<? extends U> other,
                                                              @Nonnull BiConsumer<? super V, ? super U> action) {
        return thenAcceptBothAsync(other, action, defaultExecutor());
    }

    @Override
    public <U> InternalCompletableFuture<Void> thenAcceptBothAsync(@Nonnull CompletionStage<? extends U> other,
                                                                   @Nonnull BiConsumer<? super V, ? super U> action) {
        return thenAcceptBothAsync(other, action, defaultExecutor());
    }

    @Override
    public <U> InternalCompletableFuture<Void> thenAcceptBothAsync(@Nonnull CompletionStage<? extends U> other,
                                                                   @Nonnull BiConsumer<? super V, ? super U> action,
                                                                   @Nonnull Executor executor) {
        requireNonNull(action);
        requireNonNull(executor);
        final InternalCompletableFuture<Void> future = newCompletableFuture();
        final CompletableFuture<? extends U> otherFuture =
                (other instanceof CompletableFuture) ? (CompletableFuture<? extends U>) other : other.toCompletableFuture();

        if (isDone()) {
            unblockAcceptBoth(otherFuture, action, executor, future);
        } else {
            Object result = registerWaiter(new AcceptBothNode<>(future, otherFuture, action), executor);
            if (result != UNRESOLVED) {
                unblockAcceptBoth(otherFuture, action, executor, future);
            }
        }
        return future;
    }

    @Override
    public InternalCompletableFuture<Void> runAfterBoth(@Nonnull CompletionStage<?> other, @Nonnull Runnable action) {
        return runAfterBothAsync(other, action, defaultExecutor());
    }

    @Override
    public InternalCompletableFuture<Void> runAfterBothAsync(@Nonnull CompletionStage<?> other, @Nonnull Runnable action) {
        return runAfterBothAsync(other, action, defaultExecutor());
    }

    @Override
    public InternalCompletableFuture<Void> runAfterBothAsync(@Nonnull CompletionStage<?> other,
                                                             @Nonnull Runnable action,
                                                             @Nonnull Executor executor) {
        requireNonNull(other);
        requireNonNull(action);
        requireNonNull(executor);
        final InternalCompletableFuture<Void> future = newCompletableFuture();
        final CompletableFuture<?> otherFuture =
                (other instanceof CompletableFuture) ? (CompletableFuture<?>) other : other.toCompletableFuture();

        if (isDone()) {
            unblockRunAfterBoth(otherFuture, action, executor, future);
        } else {
            Object result = registerWaiter(new RunAfterBothNode<>(future, otherFuture, action), executor);
            if (result != UNRESOLVED) {
                unblockRunAfterBoth(otherFuture, action, executor, future);
            }
        }
        return future;
    }

    @Override
    public <U> InternalCompletableFuture<U> applyToEither(@Nonnull CompletionStage<? extends V> other,
                                                          @Nonnull Function<? super V, U> fn) {
        return applyToEitherAsync(other, fn, defaultExecutor());
    }

    @Override
    public <U> InternalCompletableFuture<U> applyToEitherAsync(@Nonnull CompletionStage<? extends V> other,
                                                               @Nonnull Function<? super V, U> fn) {
        return applyToEitherAsync(other, fn, defaultExecutor());
    }

    @Override
    public <U> InternalCompletableFuture<U> applyToEitherAsync(@Nonnull CompletionStage<? extends V> other,
                                                               @Nonnull Function<? super V, U> fn,
                                                               @Nonnull Executor executor) {
        requireNonNull(other);
        requireNonNull(fn);
        requireNonNull(executor);
        final InternalCompletableFuture<U> future = newCompletableFuture();
        final CompletableFuture<? extends V> otherFuture =
                (other instanceof CompletableFuture) ? (CompletableFuture<? extends V>) other : other.toCompletableFuture();

        if (isDone()) {
            unblockApplyToEither(fn, executor, future);
        } else {
            ApplyEither<? super V, U> waiter = new ApplyEither<>(future, fn);
            Object result = registerWaiter(waiter, executor);
            if (result == UNRESOLVED) {
                otherFuture.whenCompleteAsync(waiter, executor);
                return future;
            } else {
                unblockApplyToEither(fn, executor, future);
            }
        }
        return future;
    }

    @Override
    public InternalCompletableFuture<Void> acceptEither(@Nonnull CompletionStage<? extends V> other,
                                                        @Nonnull Consumer<? super V> action) {
        return acceptEitherAsync(other, action, defaultExecutor());
    }

    @Override
    public InternalCompletableFuture<Void> acceptEitherAsync(@Nonnull CompletionStage<? extends V> other,
                                                             @Nonnull Consumer<? super V> action) {
        return acceptEitherAsync(other, action, defaultExecutor());
    }

    @Override
    public InternalCompletableFuture<Void> acceptEitherAsync(@Nonnull CompletionStage<? extends V> other,
                                                             @Nonnull Consumer<? super V> action,
                                                             @Nonnull Executor executor) {
        requireNonNull(other);
        requireNonNull(action);
        requireNonNull(executor);
        final InternalCompletableFuture<Void> future = newCompletableFuture();
        final CompletableFuture<? extends V> otherFuture =
                (other instanceof CompletableFuture) ? (CompletableFuture<? extends V>) other : other.toCompletableFuture();

        if (isDone()) {
            unblockAcceptEither(action, executor, future);
        } else {
            AcceptEither<? super V> waiter = new AcceptEither<>(future, action);
            Object result = registerWaiter(waiter, executor);
            if (result == UNRESOLVED) {
                otherFuture.whenCompleteAsync(waiter, executor);
                return future;
            } else {
                unblockAcceptEither(action, executor, future);
            }
        }
        return future;
    }

    @Override
    public InternalCompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return runAfterEitherAsync(other, action, defaultExecutor());
    }

    @Override
    public InternalCompletableFuture<Void> runAfterEitherAsync(@Nonnull CompletionStage<?> other, @Nonnull Runnable action) {
        return runAfterEitherAsync(other, action, defaultExecutor());
    }

    @Override
    public InternalCompletableFuture<Void> runAfterEitherAsync(@Nonnull CompletionStage<?> other,
                                                               @Nonnull Runnable action,
                                                               @Nonnull Executor executor) {
        requireNonNull(other);
        requireNonNull(action);
        requireNonNull(executor);

        final InternalCompletableFuture<Void> future = newCompletableFuture();
        final CompletableFuture<?> otherFuture =
                (other instanceof CompletableFuture) ? (CompletableFuture<?>) other : other.toCompletableFuture();

        if (isDone()) {
            unblockRunAfterEither(action, executor, future);
        } else {
            RunAfterEither waiter = new RunAfterEither(future, action);
            Object result = registerWaiter(waiter, executor);
            if (result == UNRESOLVED) {
                otherFuture.whenCompleteAsync(waiter, executor);
                return future;
            } else {
                unblockRunAfterEither(action, executor, future);
            }
        }
        return future;
    }

    @Override
    public InternalCompletableFuture<V> exceptionally(@Nonnull Function<Throwable, ? extends V> fn) {
        requireNonNull(fn);
        final InternalCompletableFuture<V> future = newCompletableFuture();
        if (isDone()) {
            unblockExceptionally(fn, future);
        } else {
            Object result = registerWaiter(new ExceptionallyNode<>(future, fn), null);
            if (result != UNRESOLVED) {
                unblockExceptionally(fn, future);
            }
        }
        return future;
    }

    @Override
    public InternalCompletableFuture<V> toCompletableFuture() {
        return this;
    }

    boolean compareAndSetState(Object oldState, Object newState) {
        return STATE_UPDATER.compareAndSet(this, oldState, newState);
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
        final Object response = registerWaiter(Thread.currentThread(), null);
        if (response != UNRESOLVED) {
            return resolveAndThrowWithJoinConvention(response);
        }

        boolean interrupted = false;
        try {
            do {
                manageParking(0);
                if (isDone()) {
                    return resolveAndThrowWithJoinConvention(state);
                } else if (Thread.interrupted()) {
                    interrupted = true;
                    onInterruptDetected();
                }
            } while (true);
        } finally {
            restoreInterrupt(interrupted);
        }
    }

    /**
     * Similarly to {@link #join()}, returns the value when complete or throws an unchecked exception if
     * completed exceptionally. Unlike {@link #join()}, checked exceptions are not wrapped in {@link CompletionException};
     * rather they are wrapped in {@link com.hazelcast.core.HazelcastException}s.
     *
     * @return the result
     */
    @Override
    public V joinInternal() {
        final Object response = registerWaiter(Thread.currentThread(), null);
        if (response != UNRESOLVED) {
            // no registration was done since a value is available.
            return resolveAndThrowForJoinInternal(response);
        }

        boolean interrupted = false;
        try {
            do {
                manageParking(0);
                if (isDone()) {
                    return resolveAndThrowForJoinInternal(state);
                } else if (Thread.interrupted()) {
                    interrupted = true;
                    onInterruptDetected();
                }
            } while (true);
        } finally {
            restoreInterrupt(interrupted);
        }
    }

    V resolveAndThrowForJoinInternal(Object unresolved) {
        Object resolved = resolve(unresolved);
        if (!(resolved instanceof ExceptionalResult)) {
            return (V) resolved;
        } else {
            throw sneakyThrow(((ExceptionalResult) resolved).wrapForJoinInternal());
        }
    }

    @Override
    public final V get() throws InterruptedException, ExecutionException {
        Object response = registerWaiter(Thread.currentThread(), null);
        if (response != UNRESOLVED) {
            // no registration was done since a value is available.
            return resolveAndThrowIfException(response);
        }

        boolean interrupted = false;
        try {
            do {
                manageParking(0);
                if (isDone()) {
                    return resolveAndThrowIfException(state);
                } else if (Thread.interrupted()) {
                    interrupted = true;
                    onInterruptDetected();
                }
            } while (true);
        } finally {
            restoreInterrupt(interrupted);
        }
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
                manageParking(timeoutNanos);
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

    // Use when the caller thread is a ForkJoinWorkerThread
    class ManagedBlocker implements ForkJoinPool.ManagedBlocker {

        private final long timeoutNanos;

        ManagedBlocker(long timeoutNanos) {
            this.timeoutNanos = timeoutNanos;
        }

        @Override
        public boolean isReleasable() {
            return Thread.currentThread().isInterrupted()
                    || isDone();
        }

        @Override
        public boolean block() throws InterruptedException {
            if (isReleasable()) {
                return true;
            } else if (timeoutNanos == 0) {
                park();
            } else if (timeoutNanos > 0) {
                parkNanos(timeoutNanos);
            }
            return isReleasable();
        }
    }

    private void manageParking(long timeoutNanos) {
        try {
            // if the caller thread is a ForkJoinWorkerThread
            if (ForkJoinTask.inForkJoinPool()) {
                ForkJoinPool.managedBlock(new ManagedBlocker(timeoutNanos));
            } else if (timeoutNanos == 0) {
                park();
            } else if (timeoutNanos > 0) {
                parkNanos(timeoutNanos);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public V getNow(V valueIfAbsent) {
        return isDone() ? join() : valueIfAbsent;
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        return completeExceptionallyInternal(ex);
    }

    @Override
    public void obtrudeValue(V value) {
        obtrude0(value);
    }

    @Override
    public void obtrudeException(Throwable ex) {
        obtrude0(wrapThrowable(ex));
    }

    private void obtrude0(Object value) {
        for (; ; ) {
            final Object oldState = state;
            if (compareAndSetState(oldState, value)) {
                onComplete();
                unblockAll(oldState, defaultExecutor());
                break;
            }
        }
    }

    @Override
    public int getNumberOfDependents() {
        int dependents = 0;
        Object index = state;
        while (index instanceof WaitNode) {
            dependents++;
            index = ((WaitNode) index).next;
        }
        if (index instanceof Waiter) {
            // the first dependent registered with a default executor
            // is not wrapped in a WaitNode
            dependents++;
        }
        return dependents;
    }

    private void unblockAll(Object waiter, Executor executor) {
        while (waiter != null) {
            if (waiter instanceof Thread) {
                unpark((Thread) waiter);
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

    private void unblockAccept(@Nonnull final Consumer<? super V> consumer,
                               @Nonnull Executor executor,
                               @Nonnull InternalCompletableFuture<Void> future) {
        final Object value = resolve(state);
        if (cascadeException(value, future)) {
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    consumer.accept((V) value);
                    future.complete(null);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (RejectedExecutionException e) {
            future.completeExceptionally(wrapToInstanceNotActiveException(e));
        }
    }

    /**
     * @param waiter   the current wait node, see javadoc of {@link #state state field}
     * @param executor the {@link Executor} on which to execute the action associated with {@code waiter}
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    protected void unblockOtherNode(Object waiter, Executor executor) {
        if (!(waiter instanceof Waiter)) {
            return;
        }
        Object value = resolve(state);
        if (waiter instanceof UniWaiter) {
            ((UniWaiter) waiter).execute(executor, value);
        } else if (waiter instanceof BiWaiter) {
            Throwable t = (value instanceof ExceptionalResult) ? ((ExceptionalResult) value).cause : null;
            value = (value instanceof ExceptionalResult) ? null : value;
            ((BiWaiter) waiter).execute(executor, value, t);
        } else if (waiter instanceof ExceptionallyNode) {
            ((ExceptionallyNode) waiter).execute(value);
        }
    }

    protected abstract Exception wrapToInstanceNotActiveException(RejectedExecutionException e);

    protected V returnOrThrowWithJoinConventions(Object resolved) {
        if (!(resolved instanceof ExceptionalResult)) {
            return (V) resolved;
        }
        Throwable cause = ((ExceptionalResult) resolved).cause;
        if (cause instanceof CancellationException) {
            throw (CancellationException) cause;
        } else if (cause instanceof CompletionException) {
            throw (CompletionException) cause;
        }
        throw new CompletionException(cause);
    }

    /**
     * @param value the resolved state of this future
     * @return an {@link ExceptionalResult} wrapping a {@link Throwable} in case value is resolved
     * to an exception, or the normal completion value. Subclasses may choose to treat
     * specific normal completion values in a special way (eg deserialize when the completion
     * value is an instance of {@code Data}.
     */
    protected Object resolve(Object value) {
        return value;
    }

    protected V resolveAndThrowWithJoinConvention(Object state) {
        Object value = resolve(state);
        return returnOrThrowWithJoinConventions(value);
    }

    protected <U> void unblockApply(@Nonnull final Function<? super V, ? extends U> function,
                                    @Nonnull Executor executor,
                                    @Nonnull InternalCompletableFuture<U> future) {
        final Object value = resolve(state);
        if (cascadeException(value, future)) {
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    U result = function.apply((V) value);
                    future.complete(result);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (RejectedExecutionException e) {
            future.completeExceptionally(wrapToInstanceNotActiveException(e));
        }
    }

    protected void unblockRun(@Nonnull final Runnable runnable,
                              @Nonnull Executor executor,
                              @Nonnull CompletableFuture<Void> future) {
        final Object value = resolve(state);
        if (cascadeException(value, future)) {
            return;
        }
        runAfter0(future, runnable, executor);
    }

    protected <U> void unblockHandle(@Nonnull BiFunction<? super V, Throwable, ? extends U> fn,
                                     @Nonnull Executor executor,
                                     @Nonnull CompletableFuture<U> future) {
        final Object result = resolve(state);
        V value;
        Throwable throwable;
        if (result instanceof ExceptionalResult) {
            throwable = ((ExceptionalResult) result).getCause();
            value = null;
        } else {
            throwable = null;
            value = (V) result;
        }

        try {
            executor.execute(() -> {
                try {
                    U r = fn.apply(value, throwable);
                    future.complete(r);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (RejectedExecutionException e) {
            future.completeExceptionally(wrapToInstanceNotActiveException(e));
        }
    }

    protected void unblockWhenComplete(@Nonnull final BiConsumer<? super V, ? super Throwable> biConsumer,
                                       @Nonnull Executor executor,
                                       @Nonnull CompletableFuture<V> future) {
        Object result = resolve(state);
        V value;
        Throwable throwable;
        if (result instanceof ExceptionalResult) {
            throwable = ((ExceptionalResult) result).cause;
            value = null;
        } else {
            throwable = null;
            value = (V) result;
        }

        try {
            executor.execute(() -> {
                try {
                    biConsumer.accept((V) value, throwable);
                } catch (Throwable t) {
                    completeDependentExceptionally(future, throwable, t);
                    return;
                }
                completeDependent(future, value, throwable);
            });
        } catch (RejectedExecutionException e) {
            future.completeExceptionally(wrapToInstanceNotActiveException(e));
        }
    }

    private void unblockExceptionally(@Nonnull Function<Throwable, ? extends V> fn,
                                      InternalCompletableFuture<V> future) {
        Object result = resolve(state);
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
    }

    protected <U> void unblockCompose(@Nonnull final Function<? super V, ? extends CompletionStage<U>> function,
                                      @Nonnull Executor executor,
                                      @Nonnull CompletableFuture<U> future) {
        Object result = resolve(state);
        if (cascadeException(result, future)) {
            return;
        }
        final V res = (V) result;
        try {
            executor.execute(() -> {
                try {
                    CompletionStage<U> r = function.apply(res);
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
        } catch (RejectedExecutionException e) {
            future.completeExceptionally(wrapToInstanceNotActiveException(e));
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    protected <U, R> void unblockCombine(@Nonnull CompletionStage<? extends U> other,
                                         @Nonnull final BiFunction<? super V, ? super U, ? extends R> function,
                                         @Nonnull Executor executor,
                                         @Nonnull InternalCompletableFuture<R> future) {
        Object result = resolve(state);
        final CompletableFuture<? extends U> otherFuture =
                (other instanceof CompletableFuture) ? (CompletableFuture<? extends U>) other : other.toCompletableFuture();

        // CompletionStage#thenCombine specifies to wait both futures for normal completion,
        // but does not specify that it is required to wait for both when completed exceptionally.
        // The CompletableFuture#thenCombine implementation actually waits both future completion
        // even when one of them is completed exceptionally.
        // In case this future is completed exceptionally, the result is also exceptionally
        // completed without checking whether otherFuture is completed or not
        if (cascadeException(result, future)) {
            return;
        }
        final V value = (V) result;
        if (!otherFuture.isDone()) {
            // register on other future as waiter and return
            otherFuture.whenCompleteAsync((v, t) -> {
                if (t != null) {
                    future.completeExceptionally(t);
                }
                try {
                    R r = function.apply(value, v);
                    future.complete(r);
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                }
            }, executor);
            return;
        }
        // both futures are done
        if (otherFuture.isCompletedExceptionally()) {
            otherFuture.whenComplete((v, t) -> {
                future.completeExceptionally(t);
            });
            return;
        }
        U otherValue = otherFuture.join();
        try {
            executor.execute(() -> {
                try {
                    R r = function.apply(value, otherValue);
                    future.complete(r);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (RejectedExecutionException e) {
            future.completeExceptionally(wrapToInstanceNotActiveException(e));
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private <U> void unblockAcceptBoth(@Nonnull CompletableFuture<? extends U> otherFuture,
                                       @Nonnull final BiConsumer<? super V, ? super U> action,
                                       @Nonnull Executor executor,
                                       @Nonnull InternalCompletableFuture<Void> future) {
        final Object value = resolve(state);
        // in case this future is completed exceptionally, the result is also exceptionally completed
        // without checking whether otherFuture is completed or not
        if (cascadeException(value, future)) {
            return;
        }
        if (!otherFuture.isDone()) {
            // register on other future as waiter and return
            otherFuture.whenCompleteAsync((u, t) -> {
                if (t != null) {
                    future.completeExceptionally(t);
                }
                try {
                    action.accept((V) value, u);
                    future.complete(null);
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                }
            }, executor);
            return;
        }
        // both futures are done
        if (otherFuture.isCompletedExceptionally()) {
            otherFuture.whenComplete((v, t) -> {
                future.completeExceptionally(t);
            });
            return;
        }
        U otherValue = otherFuture.join();
        try {
            executor.execute(() -> {
                try {
                    action.accept((V) value, otherValue);
                    future.complete(null);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (RejectedExecutionException e) {
            future.completeExceptionally(wrapToInstanceNotActiveException(e));
        }
    }

    private void unblockRunAfterBoth(@Nonnull CompletableFuture<?> otherFuture,
                                     @Nonnull final Runnable action,
                                     @Nonnull Executor executor,
                                     @Nonnull CompletableFuture<Void> future) {
        Object result = resolve(state);
        // in case this future is completed exceptionally, the result is also exceptionally completed
        // without checking whether otherFuture is completed or not
        if (cascadeException(result, future)) {
            return;
        }
        if (!otherFuture.isDone()) {
            // register on other future as waiter and return
            otherFuture.whenCompleteAsync((u, t) -> {
                if (t != null) {
                    future.completeExceptionally(t);
                }
                try {
                    action.run();
                    future.complete(null);
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                }
            }, executor);
            return;
        }
        // both futures are done
        if (otherFuture.isCompletedExceptionally()) {
            otherFuture.whenComplete((v, t) -> {
                future.completeExceptionally(t);
            });
            return;
        }
        runAfter0(future, action, executor);
    }

    protected <U> void unblockApplyToEither(@Nonnull final Function<? super V, U> action,
                                            @Nonnull Executor executor,
                                            @Nonnull CompletableFuture<U> future) {
        Object result = resolve(state);
        if (cascadeException(result, future)) {
            return;
        }
        applyTo0(future, action, executor, (V) result);
    }

    protected void unblockAcceptEither(@Nonnull final Consumer<? super V> action,
                                       @Nonnull Executor executor,
                                       @Nonnull CompletableFuture<Void> future) {
        Object result = resolve(state);
        if (cascadeException(result, future)) {
            return;
        }
        acceptAfter0(future, action, executor, (V) result);
    }

    protected CompletableFuture<Void> unblockRunAfterEither(@Nonnull final Runnable action,
                                                            @Nonnull Executor executor,
                                                            @Nonnull CompletableFuture<Void> future) {
        Object result = resolve(state);
        if (cascadeException(result, future)) {
            return future;
        }
        return runAfter0(future, action, executor);
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
            if (oldState == UNRESOLVED && (executor == null || executor == defaultExecutor())) {
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
                unblockAll(oldState, defaultExecutor());
                return true;
            }
        }
    }

    protected void onComplete() {
        if (state instanceof ExceptionalResult) {
            super.completeExceptionally(((ExceptionalResult) state).getCause());
        } else {
            super.complete((V) state);
        }
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

    private CompletableFuture<Void> runAfter0(@Nonnull CompletableFuture<Void> result,
                                              @Nonnull Runnable action,
                                              @Nonnull Executor executor) {
        try {
            executor.execute(() -> {
                try {
                    action.run();
                    result.complete(null);
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                }
            });
        } catch (RejectedExecutionException e) {
            result.completeExceptionally(wrapToInstanceNotActiveException(e));
        }
        return result;
    }

    private CompletableFuture<Void> acceptAfter0(@Nonnull CompletableFuture<Void> result,
                                                 @Nonnull Consumer<? super V> consumer,
                                                 @Nonnull Executor executor,
                                                 V value) {
        try {
            executor.execute(() -> {
                try {
                    consumer.accept(value);
                    result.complete(null);
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                }
            });
        } catch (RejectedExecutionException e) {
            result.completeExceptionally(wrapToInstanceNotActiveException(e));
        }
        return result;
    }

    private <U> CompletableFuture<U> applyTo0(@Nonnull CompletableFuture<U> future,
                                              @Nonnull Function<? super V, U> consumer,
                                              @Nonnull Executor executor,
                                              V value) {
        try {
            executor.execute(() -> {
                try {
                    future.complete(consumer.apply(value));
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (RejectedExecutionException e) {
            future.completeExceptionally(wrapToInstanceNotActiveException(e));
        }
        return future;
    }

    <T> InternalCompletableFuture<T> newCompletableFuture() {
        return new InternalCompletableFuture<>();
    }

    /**
     * If {@code resolved} is an {@link ExceptionalResult}, complete the {@code dependent}
     * exceptionally with a {@link CompletionException} that wraps the cause.
     * Used as discussed in {@link CompletionStage} javadoc regarding exceptional completion
     * of dependents.
     *
     * @param resolved  a resolved state, as returned from {@link #resolve(Object)}
     * @param dependent a dependent {@link CompletableFuture}
     * @return {@code true} in case the dependent was completed exceptionally, otherwise {@code false}
     */
    private static boolean cascadeException(Object resolved, CompletableFuture dependent) {
        if (resolved instanceof ExceptionalResult) {
            dependent.completeExceptionally(wrapInCompletionException((((ExceptionalResult) resolved).cause)));
            return true;
        }
        return false;
    }

    private static CompletionException wrapInCompletionException(Throwable t) {
        return (t instanceof CompletionException)
                ? (CompletionException) t
                : new CompletionException(t);
    }

    protected static ExceptionalResult wrapThrowable(Object value) {
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
        private final @Nonnull
        Executor executor;

        WaitNode(Object waiter, @Nullable Executor executor) {
            this.waiter = waiter;
            this.executor = executor == null ? ConcurrencyUtil.getDefaultAsyncExecutor() : executor;
        }

        @Override
        public String toString() {
            return "WaitNode{" + "waiter=" + waiter + ", next=" + next + ", executor=" + executor + '}';
        }
    }

    public static final class ExceptionalResult {
        private final Throwable cause;

        public ExceptionalResult(Throwable cause) {
            this.cause = cause;
        }

        public Throwable getCause() {
            return cause;
        }

        /**
         * Wraps the {@link #cause} so that the remote/async throwable is not lost,
         * however is delivered as the cause to an throwable with a local stack trace
         * that makes sense to user code that is synchronizing on {@code joinInternal()}.
         * <p>
         * Exception wrapping rules:
         * <ul>
         *     <li>
         *         if cause is an instance of {@link RuntimeException} then the cause is cloned
         *         The clone throwable has the local stack trace merged into to the original stack trace
         *     </li>
         *     <li>
         *         if cause is an instance of {@link ExecutionException} or {@link InvocationTargetException}
         *         with a non-null cause, then unwrap and apply the rules for the cause
         *     </li>
         *     <li>
         *         if cause is an {@link Error}, then the cause is cloned.
         *         The clone throwable has the local stack trace merged into to the original stack trace
         *     </li>
         *     <li>
         *         otherwise, wrap cause in a {@link HazelcastException} reporting the local stack trace,
         *         while the remote stack trace is reported in its cause exception.
         *     </li>
         * </ul>
         *
         * @return
         */
        public Throwable wrapForJoinInternal() {
            return wrapOrPeel(cause);
        }

        @Override
        public String toString() {
            return "ExceptionalResult{" + "cause=" + cause + '}';
        }
    }

    /**
     * Marker interface for completions registered on a yet incomplete future.
     * Also extends {@link java.util.concurrent.CompletableFuture.AsynchronousCompletionTask}
     * for monitoring & debugging.
     */
    interface Waiter extends AsynchronousCompletionTask {
    }

    /**
     * Interface for dependent stages registered on a yet incomplete future
     * which perform some action ({@code Function}, {@code Consumer}...) on this
     * future's resolved value.
     */
    interface UniWaiter extends Waiter {
        void execute(@Nonnull Executor executor, Object value);
    }

    /**
     * Interface for dependent stages registered on a yet incomplete future
     * which perform some action ({@code BiFunction}, {@code BiConsumer}..)
     * on the normal or exceptional completion value.
     */
    interface BiWaiter<V, T extends Throwable> extends Waiter {
        void execute(@Nonnull Executor executor, V value, T throwable);
    }

    // a WaitNode for a Function<V, R>
    protected final class ApplyNode<V, R> implements UniWaiter {
        final CompletableFuture<R> future;
        final Function<V, R> function;

        ApplyNode(CompletableFuture<R> future, Function<V, R> function) {
            this.future = future;
            this.function = function;
        }

        @Override
        public void execute(@Nonnull Executor executor, Object value) {
            if (cascadeException(value, future)) {
                return;
            }
            try {
                executor.execute(() -> {
                    try {
                        future.complete(function.apply((V) value));
                    } catch (Throwable t) {
                        future.completeExceptionally(t);
                    }
                });
            } catch (RejectedExecutionException e) {
                future.completeExceptionally(wrapToInstanceNotActiveException(e));
                throw e;
            }
        }
    }

    // a WaitNode for exceptionally(Function<Throwable, V>)
    protected static final class ExceptionallyNode<R> implements Waiter {
        final CompletableFuture<R> future;
        final Function<Throwable, ? extends R> function;

        ExceptionallyNode(CompletableFuture<R> future, Function<Throwable, ? extends R> function) {
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

    // a WaitNode for a BiFunction<V, Throwable, R>
    private final class HandleNode<V, R> implements BiWaiter<V, Throwable> {
        final CompletableFuture<R> future;
        final BiFunction<V, Throwable, R> biFunction;

        HandleNode(CompletableFuture<R> future, BiFunction<V, Throwable, R> biFunction) {
            this.future = future;
            this.biFunction = biFunction;
        }

        @Override
        public void execute(@Nonnull Executor executor, V value, Throwable throwable) {
            try {
                executor.execute(() -> {
                    try {
                        future.complete(biFunction.apply(value, throwable));
                    } catch (Throwable t) {
                        future.completeExceptionally(t);
                    }
                });
            } catch (RejectedExecutionException exception) {
                future.completeExceptionally(wrapToInstanceNotActiveException(exception));
                throw exception;
            }
        }
    }

    // a WaitNode for a BiConsumer<V, T>
    private final class WhenCompleteNode<V, T extends Throwable> implements BiWaiter<V, T> {
        final CompletableFuture<V> future;
        final BiConsumer<V, T> biConsumer;

        WhenCompleteNode(@Nonnull CompletableFuture<V> future, @Nonnull BiConsumer<V, T> biConsumer) {
            this.future = future;
            this.biConsumer = biConsumer;
        }

        @Override
        public void execute(@Nonnull Executor executor, V value, T throwable) {
            try {
                executor.execute(() -> {
                    try {
                        biConsumer.accept(value, throwable);
                    } catch (Throwable t) {
                        completeDependentExceptionally(future, throwable, t);
                        return;
                    }
                    complete(value, throwable);
                });
            } catch (RejectedExecutionException exception) {
                future.completeExceptionally(wrapToInstanceNotActiveException(exception));
                throw exception;
            }
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
    private final class AcceptNode<T> implements UniWaiter {
        final CompletableFuture<Void> future;
        final Consumer<T> consumer;

        AcceptNode(@Nonnull CompletableFuture<Void> future, @Nonnull Consumer<T> consumer) {
            this.future = future;
            this.consumer = consumer;
        }

        @Override
        public void execute(@Nonnull Executor executor, Object value) {
            if (cascadeException(value, future)) {
                return;
            }
            try {
                executor.execute(() -> {
                    try {
                        consumer.accept((T) value);
                        future.complete(null);
                    } catch (Throwable t) {
                        future.completeExceptionally(t);
                    }
                });
            } catch (RejectedExecutionException exception) {
                future.completeExceptionally(wrapToInstanceNotActiveException(exception));
                throw exception;
            }
        }
    }

    // a WaitNode for a Runnable
    protected final class RunNode implements UniWaiter {
        final CompletableFuture<Void> future;
        final Runnable runnable;

        RunNode(@Nonnull CompletableFuture<Void> future, @Nonnull Runnable runnable) {
            this.future = future;
            this.runnable = runnable;
        }

        @Override
        public void execute(@Nonnull Executor executor, Object resolved) {
            if (cascadeException(resolved, future)) {
                return;
            }
            try {
                executor.execute(() -> {
                    try {
                        runnable.run();
                        future.complete(null);
                    } catch (Throwable t) {
                        future.completeExceptionally(t);
                    }
                });
            } catch (RejectedExecutionException exception) {
                future.completeExceptionally(wrapToInstanceNotActiveException(exception));
                throw exception;
            }
        }
    }

    protected final class ComposeNode<T, U> implements UniWaiter {
        final CompletableFuture<U> future;
        final Function<? super T, ? extends CompletionStage<U>> function;

        ComposeNode(CompletableFuture<U> future, Function<? super T, ? extends CompletionStage<U>> function) {
            this.future = future;
            this.function = function;
        }

        @Override
        public void execute(@Nonnull Executor executor, Object resolved) {
            if (cascadeException(resolved, future)) {
                return;
            }
            try {
                executor.execute(() -> {
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
            } catch (RejectedExecutionException exception) {
                future.completeExceptionally(wrapToInstanceNotActiveException(exception));
                throw exception;
            }
        }
    }

    // common superclass of waiters for two futures (combine, acceptBoth, runAfterBoth)
    protected abstract class AbstractBiNode<T, U, R> implements UniWaiter {
        final CompletableFuture<R> result;
        final CompletableFuture<? extends U> otherFuture;
        final AtomicBoolean executed;

        AbstractBiNode(CompletableFuture<R> future,
                       CompletableFuture<? extends U> otherFuture) {
            this.result = future;
            this.otherFuture = otherFuture;
            this.executed = new AtomicBoolean();
        }

        @Override
        @SuppressWarnings("checkstyle:npathcomplexity")
        public void execute(@Nonnull Executor executor, Object resolved) {
            if (cascadeException(resolved, result)) {
                return;
            }
            if (!otherFuture.isDone()) {
                // register on other future and exit
                otherFuture.whenCompleteAsync((u, t) -> {
                    if (!executed.compareAndSet(false, true)) {
                        return;
                    }
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
            if (!executed.compareAndSet(false, true)) {
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
            try {
                executor.execute(() -> {
                    try {
                        R r = process((T) resolved, otherValue);
                        result.complete(r);
                    } catch (Throwable t) {
                        result.completeExceptionally(t);
                    }
                });
            } catch (RejectedExecutionException exception) {
                result.completeExceptionally(wrapToInstanceNotActiveException(exception));
                throw exception;
            }
        }

        abstract R process(T t, U u);
    }

    private final class CombineNode<T, U, R> extends AbstractBiNode<T, U, R> {
        final BiFunction<? super T, ? super U, ? extends R> function;

        CombineNode(CompletableFuture<R> future,
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

    private final class AcceptBothNode<T, U> extends AbstractBiNode<T, U, Void> {
        final BiConsumer<? super T, ? super U> action;

        AcceptBothNode(CompletableFuture<Void> future,
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

    private final class RunAfterBothNode<T, U> extends AbstractBiNode<T, U, Void> {
        final Runnable action;

        RunAfterBothNode(CompletableFuture<Void> future,
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
    protected abstract class AbstractEitherNode<T, R> implements UniWaiter, BiConsumer<T, Throwable> {
        final CompletableFuture<R> result;
        final AtomicBoolean executed;

        AbstractEitherNode(CompletableFuture<R> future) {
            this.result = future;
            this.executed = new AtomicBoolean();
        }

        @Override
        public void execute(@Nonnull Executor executor, Object resolved) {
            if (!executed.compareAndSet(false, true)) {
                return;
            }
            if (cascadeException(resolved, result)) {
                return;
            }
            try {
                executor.execute(() -> {
                    try {
                        R r = process((T) resolved);
                        result.complete(r);
                    } catch (Throwable t) {
                        result.completeExceptionally(t);
                    }
                });
            } catch (RejectedExecutionException exception) {
                result.completeExceptionally(wrapToInstanceNotActiveException(exception));
                throw exception;
            }
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

    private final class RunAfterEither<T> extends AbstractEitherNode<T, Void> {
        final Runnable action;

        RunAfterEither(CompletableFuture<Void> future, Runnable action) {
            super(future);
            this.action = action;
        }

        @Override
        Void process(T t) {
            action.run();
            return null;
        }
    }

    private final class AcceptEither<T> extends AbstractEitherNode<T, Void> {
        final Consumer<T> action;

        AcceptEither(CompletableFuture<Void> future, Consumer<T> action) {
            super(future);
            this.action = action;
        }

        @Override
        Void process(T t) {
            action.accept(t);
            return null;
        }
    }

    private final class ApplyEither<T, R> extends AbstractEitherNode<T, R> {
        final Function<T, R> action;

        ApplyEither(CompletableFuture<R> future, Function<T, R> action) {
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

    /**
     * Completes dependent {@code future} exceptionally. When the parent future was completed exceptionally,
     * then dependent future is also completed exceptionally with a {@link CompletionException} wrapping
     * {@code exceptionFromParent}. Otherwise, the dependent future is completed exceptionally with
     * the exception thrown from user action ({@code exceptionFromAction}).
     */
    private static void completeDependentExceptionally(CompletableFuture future, Throwable exceptionFromParent,
                                                       Throwable exceptionFromAction) {
        assert (exceptionFromParent != null || exceptionFromAction != null);
        if (exceptionFromParent == null) {
            future.completeExceptionally(exceptionFromAction);
        } else {
            future.completeExceptionally(wrapInCompletionException(exceptionFromParent));
        }
    }

    /**
     * Completes dependent future {@code future} with the given {@code throwable} wrapped in
     * {@code CompletionException}, if {@code throwable} is not {@code null}, or with the given
     * {@code value}.
     */
    private static <V> void completeDependent(CompletableFuture<V> future, V value, Throwable throwable) {
        if (throwable == null) {
            future.complete(value);
        } else {
            future.completeExceptionally(wrapInCompletionException(throwable));
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

    static Throwable wrapOrPeel(Throwable cause) {
        if (cause instanceof RuntimeException) {
            return wrapRuntimeException((RuntimeException) cause);
        }
        if ((cause instanceof ExecutionException || cause instanceof InvocationTargetException)
                && cause.getCause() != null) {
            return wrapOrPeel(cause.getCause());
        }
        if (cause instanceof Error) {
            if (cause instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) cause);
            }
            return wrapError((Error) cause);
        }
        return new HazelcastException(cause);
    }

    private static RuntimeException wrapRuntimeException(RuntimeException cause) {
        if (cause instanceof WrappableException) {
            return ((WrappableException) cause).wrap();
        }
        RuntimeException wrapped = cloneExceptionWithFixedAsyncStackTrace(cause);
        return wrapped == null ? new HazelcastException(cause) : wrapped;
    }

    private static Error wrapError(Error cause) {
        Error result = cloneExceptionWithFixedAsyncStackTrace(cause);
        return result == null ? cause : result;
    }
}
