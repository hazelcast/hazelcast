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

package com.hazelcast.function;

import com.hazelcast.internal.serialization.BinaryInterface;

final class ComparatorsEx {

    static final ComparatorEx<Comparable<Object>> NATURAL_ORDER = new NaturalOrderComparator();
    static final ComparatorEx<Comparable<Object>> REVERSE_ORDER = new ReverseOrderComparator();

    private ComparatorsEx() {
    }

    @BinaryInterface
    private static final class NaturalOrderComparator implements ComparatorEx<Comparable<Object>> {

        @Override
        public int compareEx(Comparable<Object> left, Comparable<Object> right) {
            return left.compareTo(right);
        }

        @Override
        public ComparatorEx<Comparable<Object>> reversed() {
            return REVERSE_ORDER;
        }

        private Object readResolve() {
            return NATURAL_ORDER;
        }
    }

    @BinaryInterface
    private static final class ReverseOrderComparator implements ComparatorEx<Comparable<Object>> {

        @Override
        public int compareEx(Comparable<Object> left, Comparable<Object> right) {
            return right.compareTo(left);
        }

        @Override
        public ComparatorEx<Comparable<Object>> reversed() {
            return NATURAL_ORDER;
        }

        private Object readResolve() {
            return REVERSE_ORDER;
        }
    }

    @BinaryInterface
    public static final class NullComparator<T> implements ComparatorEx<T> {
        private final boolean isNullFirst;

        @SuppressWarnings("unchecked")
        NullComparator(boolean isNullFirst) {
            this.isNullFirst = isNullFirst;
        }

        @Override
        public int compareEx(T left, T right) {
            if (left == null) {
                return (right == null) ? 0 : (isNullFirst ? -1 : 1);
            } else if (right == null) {
                return isNullFirst ? 1 : -1;
            } else {
                return 0;
            }
        }
    }
}
