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

package com.hazelcast.jet.stream.impl.distributed;

import com.hazelcast.jet.Distributed;

import java.util.Comparator;
import java.util.Objects;

/**
 *
 */
public final class DistributedComparators {

    public static final Distributed.Comparator<Comparable<Object>> NATURAL_ORDER_COMPARATOR = new NaturalOrderComparator();
    public static final Distributed.Comparator<Comparable<Object>> REVERSE_ORDER_COMPARATOR = new ReverseOrderComparator();

    private DistributedComparators() {
    }

    private static class NaturalOrderComparator implements Distributed.Comparator<Comparable<Object>> {

        @Override
        public int compare(Comparable<Object> c1, Comparable<Object> c2) {
            return c1.compareTo(c2);
        }

        @Override
        public Distributed.Comparator<Comparable<Object>> reversed() {
            return REVERSE_ORDER_COMPARATOR;
        }
    }

    private static class ReverseOrderComparator implements Distributed.Comparator<Comparable<Object>> {

        @Override
        public int compare(Comparable<Object> c1, Comparable<Object> c2) {
            return c2.compareTo(c1);
        }

        @Override
        public Distributed.Comparator<Comparable<Object>> reversed() {
            return NATURAL_ORDER_COMPARATOR;
        }
    }

    /**
     * Null-friendly comparators
     */
    public static final class NullComparator<T> implements Distributed.Comparator<T> {
        private static final long serialVersionUID = -7569533591570686392L;
        private final boolean nullFirst;
        // if null, non-null Ts are considered equal
        private final Comparator<T> real;

        @SuppressWarnings("unchecked")
        public NullComparator(boolean nullFirst, Comparator<? super T> real) {
            this.nullFirst = nullFirst;
            this.real = (Comparator<T>) real;
        }

        @Override
        public int compare(T a, T b) {
            if (a == null) {
                return (b == null) ? 0 : (nullFirst ? -1 : 1);
            } else if (b == null) {
                return nullFirst ? 1 : -1;
            } else {
                return (real == null) ? 0 : real.compare(a, b);
            }
        }

        @Override
        public Distributed.Comparator<T> thenComparing(Comparator<? super T> other) {
            Objects.requireNonNull(other);
            return new NullComparator<>(nullFirst, real == null ? other : real.thenComparing(other));
        }

        @Override
        public Distributed.Comparator<T> reversed() {
            return new NullComparator<>(!nullFirst, real == null ? null : real.reversed());
        }
    }
}
