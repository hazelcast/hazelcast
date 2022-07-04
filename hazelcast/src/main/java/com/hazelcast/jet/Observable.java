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

package com.hazelcast.jet;

import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.jet.function.Observer;
import com.hazelcast.jet.impl.observer.BlockingIteratorObserver;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.ringbuffer.Ringbuffer;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Represents a flowing sequence of events produced by {@linkplain
 * Sinks#observable(String) observable sinks}. To observe the events, call
 * {@link #addObserver jet.getObservable(name).addObserver(myObserver)}.
 * <p>
 * The {@code Observable} is backed by a {@link Ringbuffer}, which, once
 * created, has a fixed capacity for storing messages. It supports reading
 * by multiple {@code Observer Observers}, which will all observe the same
 * sequence of messages. A new {@code Observer} will start reading
 * automatically from the oldest sequence available. Once the capacity is
 * full, the oldest messages will be overwritten as new ones arrive.
 * <p>
 * The {@code Ringbuffer}'s capacity defaults to {@value
 * RingbufferConfig#DEFAULT_CAPACITY}, but can be changed (via the {@link
 * #configureCapacity(int)} method), as long as the {@code Ringbuffer} hasn't
 * been created yet (see the "Lifecycle" section below).
 * <p>
 * In addition to data events, the {@code Observer} can also observe
 * completion and failure events. Completion means that no further values
 * will appear in the sequence. Failure means that something went wrong
 * during the job execution .
 * <p>
 * <strong>Lifecycle</strong>
 * <p>
 * When talking about the lifecycle of an {@code Observable} (which is
 * basically just a client side object and has a lifecycle just like any
 * other POJO) it's better to actually consider the lifecycle of the
 * underlying {@code Ringbuffer}, since that is the significant
 * distributed entity.
 * <p>
 * The lifecycle of the {@code Ringbuffer} is decoupled from the lifecycle
 * of the job. The {@code Ringbuffer} is created either when the user
 * gets a reference to its equivalent {@code Observable} (through
 * {@link JetService#getObservable(String) JetService.getObservable()})
 * and registers the first {@link Observer} on it (through
 * {@link Observable#addObserver(Observer) Observable.addObserver()})
 * or when the job containing the sink for it starts executing.
 * <p>
 * The {@code Ringbuffer} must be explicitly destroyed when it's no longer
 * in use, or data will be retained in the cluster. This is done via the
 * {@link #destroy() Observable.destroy()} method. Note: even if the
 * {@code Observable} POJO gets lost and its underlying {@code Ringbuffer}
 * is leaked in the cluster, it's still possible to manually destroy
 * it later by creating another {@code Observable} instance with the same
 * name and calling {@code destroy()} on that.
 * <p>
 * <strong>Important:</strong> The same {@code Observable} must
 * <strong>not</strong> be used again in a new job since this will cause
 * completion events interleaving and causing data loss or other unexpected
 * behaviour. Using one observable name in multiple
 * {@link Sinks#observable(String) observable sinks} in the same job is
 * allowed, this will not produce multiple completion or error events (just
 * an intermingling of the results from the two sinks, but that should be
 * fine in some use cases).
 *
 * @param <T> type of the values in the sequence
 *
 * @since Jet 4.0
 */
public interface Observable<T> extends Iterable<T> {

    /**
     * Name of this instance.
     */
    @Nonnull
    String name();

    /**
     * Registers an {@link Observer} to this {@code Observable}. It will
     * receive all events currently in the backing {@link Ringbuffer} and
     * then continue receiving any future events.
     *
     * @return registration ID associated with the added {@code Observer},
     * can be used to remove the {@code Observer} later
     */
    @Nonnull
    UUID addObserver(@Nonnull Observer<T> observer);

    /**
     * Removes a previously added {@link Observer} identified by its
     * assigned registration ID. A removed {@code Observer} will not get
     * notified about further events.
     */
    void removeObserver(@Nonnull UUID registrationId);

    /**
     * Set the capacity of the underlying {@link Ringbuffer}, which defaults to
     * {@value RingbufferConfig#DEFAULT_CAPACITY}.
     * <p>
     * This method can be called only before the {@code Ringbuffer} gets
     * created. This means before any {@link Observer Observers} are added
     * to the {@code Observable} and before any jobs containing
     * {@link com.hazelcast.jet.pipeline.Sinks#observable(String) observable
     * sinks} (with the same observable name) are submitted for execution.
     * <p>
     * <strong>Important:</strong> only configure capacity once, multiple
     * configuration are currently not supported.
     *
     * @throws IllegalStateException if the {@code Ringbuffer} has already been
     * created
     */
    Observable<T> configureCapacity(int capacity);

    /**
     * Returns the configured capacity of the underlying {@link Ringbuffer}..
     * <p>
     * This method only works if the backing {@code Ringbuffer} has already
     * been created. If so, it will be queried for its actual capacity,
     * which can't be changed any longer. (Reminder: the {@code Ringbuffer}
     * gets created either when the first {@link Observer} is added or when
     * the job containing the {@link com.hazelcast.jet.pipeline.Sinks#observable(String)
     * observable sink} (with the same observable name) is submitted for
     * execution.)
     *
     * @throws IllegalStateException if the backing {@code Ringbuffer} has not
     * yet been created
     */
    int getConfiguredCapacity();

    /**
     * Returns an iterator over the sequence of events produced by this
     * {@code Observable}. If there are currently no events to observe,
     * the iterator's {@code hasNext()} and {@code next()} methods will block.
     * A completion event completes the iterator ({@code hasNext()} will return
     * false) and a failure event makes the iterator's methods throw the
     * underlying exception.
     * <p>
     * If used against an {@code Observable} populated from a streaming job,
     * the iterator will complete only in the case of an error or job
     * cancellation.
     * <p>
     * The iterator is not thread-safe.
     * <p>
     * The iterator is backed by a blocking concurrent queue which stores all
     * events until consumed.
     */
    @Nonnull @Override
    default Iterator<T> iterator() {
        BlockingIteratorObserver<T> observer = new BlockingIteratorObserver<>();
        addObserver(observer);
        return observer;
    }

    /**
     * Allows you to post-process the results of a Jet job on the client side
     * using the standard Java {@link java.util.stream Stream API}. You provide
     * a function that will receive the job results as a {@code Stream<T>} and
     * return a single result (which can in fact be another {@code Stream},
     * if so desired).
     * <p>
     * Returns a {@link CompletableFuture CompletableFuture<R>} that will become
     * completed once your function has received all the job results through
     * its {@code Stream} and returned the final result.
     * <p>
     * A trivial example is counting, like this: {@code observable.toFuture(Stream::count)},
     * however the Stream API is quite rich and you can perform arbitrary
     * transformations and aggregations.
     * <p>
     * This feature is intended to be used only on the results of a batch job.
     * On an unbounded streaming job the stream-collecting operation will never
     * reach the final result.
     *
     * @param fn transform function which takes the stream of observed values
     *           and produces an altered value from it, which could also
     *           be a stream
     */
    @Nonnull
    default <R> CompletableFuture<R> toFuture(@Nonnull Function<Stream<T>, R> fn) {
        Objects.requireNonNull(fn, "fn");

        Iterator<T> iterator = iterator();
        return CompletableFuture.supplyAsync(() -> {
            Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iterator, 0);
            return fn.apply(StreamSupport.stream(spliterator, false));
        });
    }

    /**
     * Removes all previously registered observers and destroys the backing
     * {@link Ringbuffer}.
     * <p>
     * <strong>Note:</strong> if you call this while a job that publishes to this
     * {@code Observable} is still active, it will silently create a new {@code
     * Ringbuffer} and go on publishing to it.
     */
    void destroy();

}
