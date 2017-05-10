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

import com.hazelcast.jet.function.DistributedComparator;

public final class DistributedComparators {

    public static final DistributedComparator<Comparable<Object>> NATURAL_ORDER_COMPARATOR = new NaturalOrderComparator();
    public static final DistributedComparator<Comparable<Object>> REVERSE_ORDER_COMPARATOR = new ReverseOrderComparator();

    private DistributedComparators() {
    }

    private static class NaturalOrderComparator implements DistributedComparator<Comparable<Object>> {

        @Override
        public int compare(Comparable<Object> c1, Comparable<Object> c2) {
            return c1.compareTo(c2);
        }

        @Override
        public DistributedComparator<Comparable<Object>> reversed() {
            return REVERSE_ORDER_COMPARATOR;
        }
    }

    private static class ReverseOrderComparator implements DistributedComparator<Comparable<Object>> {

        @Override
        public int compare(Comparable<Object> c1, Comparable<Object> c2) {
            return c2.compareTo(c1);
        }

        @Override
        public DistributedComparator<Comparable<Object>> reversed() {
            return NATURAL_ORDER_COMPARATOR;
        }
    }

    /**
     * Null-friendly comparator
     */
    public static final class NullComparator<T> implements DistributedComparator<T> {
        private static final long serialVersionUID = -7569533591570686392L;
        private final boolean nullFirst;

        @SuppressWarnings("unchecked")
        public NullComparator(boolean nullFirst) {
            this.nullFirst = nullFirst;
        }

        @Override
        public int compare(T a, T b) {
            if (a == null) {
                return (b == null) ? 0 : (nullFirst ? -1 : 1);
            } else if (b == null) {
                return nullFirst ? 1 : -1;
            } else {
                return 0;
            }
        }
    }
}
