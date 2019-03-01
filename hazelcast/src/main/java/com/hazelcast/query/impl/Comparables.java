/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import java.util.Comparator;

public final class Comparables {

    // TODO different canonicalization for on-heap and off-heap scenarios
    // TODO move canonicalization to index level from index store level completely (?)

    public static final Comparator<Comparable> COMPARATOR = new Comparator<Comparable>() {

        @Override
        public int compare(Comparable lhs, Comparable rhs) {
            return Comparables.compare(lhs, rhs);
        }

    };

    private Comparables() {
    }

    public static boolean equal(Comparable lhs, Comparable rhs) {
        assert lhs != null;
        if (rhs == null) {
            return false;
        }

        if (lhs.getClass() == rhs.getClass()) {
            return lhs.equals(rhs);
        }

        if (lhs instanceof Number && rhs instanceof Number) {
            return Numbers.equal((Number) lhs, (Number) rhs);
        }

        return lhs.equals(rhs);
    }

    @SuppressWarnings("unchecked")
    public static int compare(Comparable lhs, Comparable rhs) {
        if (lhs.getClass() == rhs.getClass()) {
            return lhs.compareTo(rhs);
        }

        if (lhs instanceof Number && rhs instanceof Number) {
            return Numbers.compare(lhs, rhs);
        }

        return lhs.compareTo(rhs);
    }

    public static Comparable canonicalizePreferringSpeed(Comparable value) {
        if (value instanceof Number) {
            return Numbers.canonicalizePreferringSpeed(value);
        }

        return value;
    }

    public static Comparable canonicalizePreferringSize(Comparable value) {
        if (value instanceof Number) {
            return Numbers.canonicalizePreferringSize(value);
        }

        return value;
    }

}
