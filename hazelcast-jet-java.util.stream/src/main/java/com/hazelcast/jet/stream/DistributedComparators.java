/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream;

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
}
