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

package com.hazelcast.jet.windowing;

import com.hazelcast.jet.Accumulators.MutableLong;
import com.hazelcast.jet.Accumulators.MutableReference;
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Distributed.BinaryOperator;

import javax.annotation.Nonnull;

/**
 * Utility class with factory methods for several useful windowing
 * operations.
 */
public final class WindowOperations {

    private WindowOperations() {
    }

    /**
     * Returns an operation that counts the items in the window.
     */
    public static WindowOperation<Object, MutableLong, Long> counting() {
        return WindowOperation.of(
                MutableLong::new,
                (a, i) -> a.value++,
                (a1, a2) -> {
                    a1.value = Math.addExact(a1.value, a2.value);
                    return a1;
                },
                (a1, a2) -> {
                    // with counting, value should never go below 0, so no need for subtractExact
                    a1.value -= a2.value;
                    return a1;
                },
                a -> a.value
        );
    }

    /**
     * Returns an operation that sums the items in the window.
     */
    public static WindowOperation<Long, MutableLong, Long> summingToLong() {
        return WindowOperations.summingToLong(Long::longValue);
    }

    /**
     * Returns an operation that counts the items in the window.
     */
    public static <T> WindowOperation<T, MutableLong, Long> summingToLong(
            @Nonnull Distributed.ToLongFunction<T> mapper
    ) {
        return WindowOperation.of(
                MutableLong::new,
                (a, value) -> a.value = Math.addExact(a.value, mapper.applyAsLong(value)),
                (a1, a2) -> {
                    a1.value = Math.addExact(a1.value, a2.value);
                    return a1;
                },
                (a1, a2) -> {
                    a1.value = Math.subtractExact(a1.value, a2.value);
                    return a1;
                },
                MutableLong::getValue
        );
    }

    /**
     * A reducing operation maintains a value that starts out as the
     * operation's <em>identity</em> value and is being iteratively transformed
     * by applying the <em>combining</em> function to the current value and a
     * new item's value. The item is first passed through the provided <em>
     * mapping</em> function. Since the order of application of the function to
     * stream items is unspecified, the combining function must be commutative
     * and associative to produce meaningful results.
     * <p>
     * To support O(1) maintenance of a sliding window, a <em>deducting</em> function
     * should be supplied whose effect is the opposite of the combining function,
     * removing the contribution of an item to the reduced result:
     * <pre>
     *     U acc = ... // any possible value
     *     U val = mapF.apply(item);
     *     U combined = combineF.apply(acc, val);
     *     U deducted = deductF.apply(combined, val);
     *     assert deducted.equals(acc);
     * </pre>
     *
     * @param identity the reducing operation's identity element
     * @param mapF a function to apply to the item before passing it to the combining
     *             function
     * @param combineF a function that combines a new item with the current result
     *                 and returns the new result
     * @param deductF a function that deducts the contribution of an item from the
     *                current result and returns the new result
     * @param <T> type of the stream item
     * @param <U> type of the reduced result
     */
    public static <T, U> WindowOperation<T, MutableReference<U>, U> reducing(
            U identity,
            Distributed.Function<? super T, ? extends U> mapF,
            Distributed.BinaryOperator<U> combineF,
            Distributed.BinaryOperator<U> deductF
    ) {
        return new WindowOperationImpl<>(
                () -> new MutableReference<>(identity),
                (a, t) -> a.value = combineF.apply(a.value, mapF.apply(t)),
                (a, b) -> {
                    a.value = combineF.apply(a.value, b.value);
                    return a;
                },
                deductF != null
                        ? (BinaryOperator<MutableReference<U>>) (a, b) -> {
                            a.value = deductF.apply(a.value, b.value);
                            return a;
                        }
                        : null,
                MutableReference::getValue);
    }
}
