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

package com.hazelcast.query.impl;

import java.util.Comparator;

/**
 * Provides utilities which compare and canonicalize {@link Comparable}
 * instances.
 */
public final class Comparables {

    /**
     * Provides the same comparison logic as {@link #compare} does, but in a
     * form of a {@link Comparator}.
     */
    public static final Comparator<Comparable> COMPARATOR = Comparables::compare;

    private Comparables() {
    }

    /**
     * Checks the provided {@link Comparable} instances for equality.
     * <p>
     * Special numeric comparison logic is used for {@link Double}, {@link Long},
     * {@link Float}, {@link Integer}, {@link Short} and {@link Byte}. See
     * {@link Numbers#equal(Number, Number)} for more details.
     *
     * @param lhs the left-hand side {@link Comparable}. Can't be {@code null}.
     * @param rhs the right-hand side {@link Comparable}. May be {@code null}.
     * @return {@code true} if the provided comparables are equal, {@code false}
     * otherwise.
     */
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

    /**
     * Compares the provided {@link Comparable} instances.
     * <p>
     * Special numeric comparison logic is used for {@link Double}, {@link Long},
     * {@link Float}, {@link Integer}, {@link Short} and {@link Byte}. See
     * {@link Numbers#compare(Comparable, Comparable)} for more details.
     *
     * @param lhs the left-hand side {@link Comparable}. Can't be {@code null}.
     * @param rhs the right-hand side {@link Comparable}. Can't be {@code null}.
     * @return a negative integer, zero, or a positive integer as the left-hand
     * side {@link Comparable} is less than, equal to, or greater than the
     * right-hand side {@link Comparable}.
     */
    @SuppressWarnings("unchecked")
    public static int compare(Comparable lhs, Comparable rhs) {
        assert lhs != null;
        assert rhs != null;

        if (lhs.getClass() == rhs.getClass()) {
            return lhs.compareTo(rhs);
        }

        if (lhs instanceof Number && rhs instanceof Number) {
            return Numbers.compare(lhs, rhs);
        }

        return lhs.compareTo(rhs);
    }

    /**
     * Canonicalizes the given {@link Comparable} value for the purpose of a
     * hash-based lookup.
     * <p>
     * Special numeric canonicalization logic is used for {@link Double},
     * {@link Long}, {@link Float}, {@link Integer}, {@link Short} and
     * {@link Byte}. See {@link Numbers#canonicalizeForHashLookup} for more
     * details.
     *
     * @param value the {@link Comparable} to canonicalize.
     * @return a canonical representation of the given {@link Comparable} or
     * the original {@link Comparable} if there is no special canonical
     * representation for it.
     */
    public static Comparable canonicalizeForHashLookup(Comparable value) {
        if (value instanceof Number) {
            return Numbers.canonicalizeForHashLookup(value);
        }

        return value;
    }

}
