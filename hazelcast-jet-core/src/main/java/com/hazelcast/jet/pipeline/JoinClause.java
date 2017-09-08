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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.function.DistributedFunction;

import java.io.Serializable;
import java.util.Map.Entry;

/**
 * Javadoc pending.
 */
public final class JoinClause<K, E0, E1, E1_OUT> implements Serializable {
    private final DistributedFunction<E0, K> leftKeyFn;
    private final DistributedFunction<E1, K> rightKeyFn;
    private final DistributedFunction<E1, E1_OUT> rightProjectFn;

    private JoinClause(
            DistributedFunction<E0, K> leftKeyFn,
            DistributedFunction<E1, K> rightKeyFn,
            DistributedFunction<E1, E1_OUT> rightProjectFn
    ) {
        this.leftKeyFn = leftKeyFn;
        this.rightKeyFn = rightKeyFn;
        this.rightProjectFn = rightProjectFn;
    }

    public static <K, E0, E1> JoinClause<K, E0, E1, E1> onKeys(
            DistributedFunction<E0, K> leftKeyFn,
            DistributedFunction<E1, K> rightKeyFn
    ) {
        return new JoinClause<>(leftKeyFn, rightKeyFn, DistributedFunction.identity());
    }

    public static <K, E0, E1, E1_IN extends Entry<K, E1>> JoinClause<K, E0, E1_IN, E1> joinMapEntries(
            DistributedFunction<E0, K> leftKeyFn
    ) {
        return new JoinClause<>(leftKeyFn, Entry::getKey, Entry::getValue);
    }

    public <E1_NEW_OUT> JoinClause<K, E0, E1, E1_NEW_OUT> projecting(
            DistributedFunction<E1, E1_NEW_OUT> rightProjectFn
    ) {
        return new JoinClause<>(this.leftKeyFn, this.rightKeyFn, rightProjectFn);
    }

    public DistributedFunction<E0, K> leftKeyFn() {
        return leftKeyFn;
    }

    public DistributedFunction<E1, K> rightKeyFn() {
        return rightKeyFn;
    }

    public DistributedFunction<E1, E1_OUT> rightProjectFn() {
        return rightProjectFn;
    }
}
