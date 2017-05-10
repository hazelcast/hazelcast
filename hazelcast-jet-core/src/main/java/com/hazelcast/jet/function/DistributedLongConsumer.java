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

package com.hazelcast.jet.function;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.LongConsumer;

/**
 * Represents an operation that accepts a single {@code long}-valued argument and
 * returns no result.  This is the primitive type specialization of
 * {@link DistributedConsumer} for {@code long}.  Unlike most other functional interfaces,
 * {@code LongConsumer} is expected to operate via side-effects.
 *
 * <p>This is a functional interface
 * whose functional method is {@link #accept(long)}.
 *
 * @see DistributedConsumer
 */
@FunctionalInterface
public interface DistributedLongConsumer extends LongConsumer, Serializable {
    /**
     * Returns a composed {@code LongConsumer} that performs, in sequence, this
     * operation followed by the {@code after} operation. If performing either
     * operation throws an exception, it is relayed to the caller of the
     * composed operation.  If performing this operation throws an exception,
     * the {@code after} operation will not be performed.
     *
     * @param after the operation to perform after this operation
     * @return a composed {@code LongConsumer} that performs in sequence this
     * operation followed by the {@code after} operation
     * @throws NullPointerException if {@code after} is null
     */
    default DistributedLongConsumer andThen(DistributedLongConsumer after) {
        Objects.requireNonNull(after);
        return (long t) -> {
            accept(t);
            after.accept(t);
        };
    }
}
