/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.function.DistributedFunction;

import java.util.Map.Entry;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * Specifies how to join an enriching stream to the primary stream in a
 * {@link BatchStage#hashJoin hash-join} operation. It holds three
 * primitives:
 * <ol><li>
 *     <em>left-hand key extractor</em>: extracts the join key from the primary
 *     stream
 * </li><li>
 *     <em>right-hand key extractor</em>: extracts the join key from the
 *     enriching stream
 * </li><li>
 *     <em>right-hand projection function</em>: maps the enriching stream item
 *     to the item that will be in the result of the join operation.
 * </li></ol>
 *  The primary use case for the projection function is enrichment from a
 *  map source, such as {@link Sources#map}.
 *  The enriching stream consists of map entries, but the result should
 *  contain just the vaules. In this case the projection function should be
 *  {@code Entry::getValue}. There is direct support for this case with the
 *  method {@link #joinMapEntries(DistributedFunction)}.
 *
 * @param <K> the type of the join key
 * @param <T0> the type of the left-hand stream item
 * @param <T1> the type of the right-hand stream item
 * @param <T1_OUT> the result type of the right-hand projection function
 */
public final class JoinClause<K, T0, T1, T1_OUT> {
    private final DistributedFunction<? super T0, ? extends K> leftKeyFn;
    private final DistributedFunction<? super T1, ? extends K> rightKeyFn;
    private final DistributedFunction<? super T1, ? extends T1_OUT> rightProjectFn;

    private JoinClause(
            DistributedFunction<? super T0, ? extends K> leftKeyFn,
            DistributedFunction<? super T1, ? extends K> rightKeyFn,
            DistributedFunction<? super T1, ? extends T1_OUT> rightProjectFn
    ) {
        checkSerializable(leftKeyFn, "leftKeyFn");
        checkSerializable(rightKeyFn, "rightKeyFn");
        checkSerializable(rightProjectFn, "rightProjectFn");
        this.leftKeyFn = leftKeyFn;
        this.rightKeyFn = rightKeyFn;
        this.rightProjectFn = rightProjectFn;
    }

    /**
     * Constructs and returns a join clause with the supplied left-hand and
     * right-hand key extractor functions, and with an identity right-hand
     * projection function.
     */
    public static <K, T0, T1> JoinClause<K, T0, T1, T1> onKeys(
            DistributedFunction<? super T0, ? extends K> leftKeyFn,
            DistributedFunction<? super T1, ? extends K> rightKeyFn
    ) {
        return new JoinClause<>(leftKeyFn, rightKeyFn, DistributedFunction.identity());
    }

    /**
     * A shorthand factory for the common case of hash-joining with a stream of
     * map entries. The right key extractor is {@code Map.Entry::getKey} and the
     * right-hand projection function is {@code Map.Entry::getValue}.
     *
     * @param leftKeyFn the function to extract the key from the primary stream
     * @param <K> the type of the key
     * @param <T0> the type of the primary stream
     * @param <T1_OUT> the type of the enriching stream's entry value
     */
    public static <K, T0, T1_OUT> JoinClause<K, T0, Entry<K, T1_OUT>, T1_OUT> joinMapEntries(
            DistributedFunction<? super T0, ? extends K> leftKeyFn
    ) {
        return new JoinClause<>(leftKeyFn, Entry::getKey, Entry::getValue);
    }

    /**
     * Returns a copy of this join clause, but with the right-hand projection
     * function replaced with the supplied one.
     */
    public <T1_NEW_OUT> JoinClause<K, T0, T1, T1_NEW_OUT> projecting(
            DistributedFunction<? super T1, ? extends T1_NEW_OUT> rightProjectFn
    ) {
        return new JoinClause<>(this.leftKeyFn, this.rightKeyFn, rightProjectFn);
    }

    /**
     * Returns the left-hand key extractor function.
     */
    public DistributedFunction<? super T0, ? extends K> leftKeyFn() {
        return leftKeyFn;
    }

    /**
     * Returns the right-hand key extractor function.
     */
    public DistributedFunction<? super T1, ? extends K> rightKeyFn() {
        return rightKeyFn;
    }

    /**
     * Returns the right-hand projection function.
     */
    public DistributedFunction<? super T1, ? extends T1_OUT> rightProjectFn() {
        return rightProjectFn;
    }
}
