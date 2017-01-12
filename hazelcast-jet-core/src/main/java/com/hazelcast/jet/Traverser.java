/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.impl.util.FlatMappingTraverser;

import javax.annotation.Nonnull;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Traverses over a sequence of non-{@code null} items, then starts returning
 * {@code null}. Each invocation of {@code next()} consumes and returns the next
 * item in the sequence, until it is exhausted. All subsequent invocations of
 * {@code next()} return {@code null}.
 *
 * @param <T> traversed item type
 */
@FunctionalInterface
public interface Traverser<T> {
    /**
     * @return the next item in the sequence, or {@code null} if the sequence is
     * already exhausted
     */
    T next();

    /**
     * Adds a mapping layer to this traverser. The returned traverser will emit
     * the results of applying the mapper function to this traverser's items.
     */
    default <R> Traverser<R> map(@Nonnull Function<? super T, ? extends R> mapper) {
        return () -> {
            final T t = next();
            return t != null ? mapper.apply(t) : null;
        };
    }

    /**
     * Adds a filtering layer to this traverser. The returned traverser will
     * emit the same items as this traverser, but only those that pass the given
     * predicate.
     */
    default Traverser<T> filter(@Nonnull Predicate<? super T> pred) {
        return () -> {
            for (T t; (t = next()) != null;) {
                if (pred.test(t)) {
                    return t;
                }
            }
            return null;
        };
    }

    /**
     * Adds a flat-mapping layer to this traverser. The returned traverser
     * will apply the given mapping function to each item retrieved from this
     * traverser, and will emit all the items from the resulting traverser(s).
     */
    default <R> Traverser<R> flatMap(@Nonnull Function<? super T, ? extends Traverser<? extends R>> mapper) {
        return new FlatMappingTraverser<>(this, mapper);
    }
}
