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

package com.hazelcast.jet.function;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.Observable;
import com.hazelcast.ringbuffer.Ringbuffer;

import javax.annotation.Nonnull;

/**
 * Observes the events produced by an {@link Observable}. Once registered,
 * it will receive all events currently in the backing {@link Ringbuffer}
 * and then continue receiving any future events.
 * <p>
 * Jet calls this {@code Observer}'s callbacks on an internal thread pool
 * of limited size, shared with many other Hazelcast Jet services. Therefore
 * the callbacks should take care to finish as quickly as possible.
 *
 * @param <T> type of the observed event
 *
 * @since Jet 4.0
 */
@FunctionalInterface
public interface Observer<T> {

    /**
     * Utility method for building an {@link Observer} from its basic
     * callback components.
     */
    @Nonnull
    static <T> Observer<T> of(
            @Nonnull ConsumerEx<? super T> onNext,
            @Nonnull ConsumerEx<? super Throwable> onError,
            @Nonnull RunnableEx onComplete
    ) {
        return new Observer<T>() {
            @Override
            public void onNext(@Nonnull T t) {
                onNext.accept(t);
            }

            @Override
            public void onError(@Nonnull Throwable throwable) {
                onError.accept(throwable);
            }

            @Override
            public void onComplete() {
                onComplete.run();
            }
        };
    }

    /**
     * Utility method for building an {@link Observer} only from its data
     * callback, with default behaviour for completion & error.
     */
    @Nonnull
    static <T> Observer<T> of(@Nonnull ConsumerEx<? super T> onNext) {
        return onNext::accept;
    }

    /**
     * Observes the next event from the {@link Observable} it is registered
     * with.
     * <p>
     * Although the {@code Observable} respects the order of events published
     * to it, the publication order itself is generally non-deterministic. A
     * Jet pipeline must be written with the specific goal of order
     * preservation in mind. For example, a map/filter stage that doesn't
     * explicitly set its local parallelism to 1 will reorder the data.
     * <p>
     * After this observer has seen an error or completion event, it will
     * see no further events.
     */
    void onNext(@Nonnull T t);

    /**
     * Observes an error event from the {@link Observable} it is registered
     * with, in the form of an exception that reflects the underlying cause.
     * <p>
     * After this observer has seen an error or completion event, it will
     * see no further events.
     */
    default void onError(@Nonnull Throwable throwable) {
        throwable.printStackTrace();
    }

    /**
     * Observes the completion event from the {@link Observable} it is
     * registered with. Only an observer attached to a batch job will ever see
     * this event. Unbounded streaming jobs never complete without error.
     * <p>
     * After this observer has seen an error or completion event, it will
     * see no further events.
     */
    default void onComplete() {
        //do nothing
    }
}
