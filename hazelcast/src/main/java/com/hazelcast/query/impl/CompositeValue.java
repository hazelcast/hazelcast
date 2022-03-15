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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.predicates.PredicateDataSerializerHook;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;

import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static java.lang.Integer.signum;

/**
 * Represents a composite value composed out of a value tuple called components.
 * <p>
 * Composite values are {@link IdentifiedDataSerializable} to facilitate the
 * storage of composite values in composite indexes, but they are never
 * transferred over the wire.
 * <p>
 * Composite values are {@link Comparable} and ordered according to a
 * lexicographical order defined by the ordering of their individual components.
 * There are 3 special values defined for components:
 * <ol>
 * <li>{@link #NEGATIVE_INFINITY} which represents a value that is equal to
 * itself and less than any other value. Negative infinities are never stored in
 * indexes and used only for querying.
 * <li>{@link AbstractIndex#NULL} which represents a null-like value that is
 * equal to itself and less than any other value except {@link
 * #NEGATIVE_INFINITY}. Null values may be stored in indexes.
 * <li>{@link #POSITIVE_INFINITY} which represents a value that is equal to
 * itself and greater than any other value. Positive infinities are never stored
 * in indexes and used only for querying.
 * </ol>
 * Basically, these special values allow to define two isolated regions in the
 * index key space: (-inf, null] and (null, +inf). The first one is used for the
 * null equality queries and the second one is used for the regular equality and
 * range queries. This separation is needed to ensure that null values are
 * excluded from the regular range queries like "a &lt; 0".
 * <p>
 * A typical ordered index key space layout for the attributes a, b and c looks
 * like this:
 * <pre>
 *    a     b     c
 * null  null  null
 * null  null   1.0
 *    1   'b'   0.0
 *    1   'b'   0.5
 *    2   'a'   null
 * </pre>
 * Notice that the ordering of the keys is suitable for the purpose of querying
 * only if keys share some common prefix. In the most general case, any query to
 * an ordered index may be reduced to a range query:
 * <pre>
 * a = null and b = null and c = null
 * [null, null, null] &lt;= [a, b, c] &lt;= [null, null, null]
 *
 * a = 2
 * [2, -inf, -inf] &lt; [a, b, c] &lt; [2, +inf, +inf]
 *
 * a = 1 and b = 'b' and c &gt; 0.0
 * [1, 'b', 0.0] &lt; [a, b, c] &lt; [1, 'b', +inf]
 *
 * a &lt; 2
 * [null, -inf, -inf] &lt; [a, b, c] &lt; [2, -inf, -inf]
 *
 * a = 1 and b &gt;= 'a' and b &lt; 'z'
 * [1, 'a', -inf] &lt; [a, b, c] &lt; [1, 'z', -inf]
 * </pre>
 * The later two examples may look counterintuitive at first, but consider what
 * is the first key that is greater than any other key having [1, 'a'] as a
 * prefix? It's [1, 'a', -inf]. The same applies to [1, 'z', -inf], it's the
 * first key that is less than any other key having [1, 'z'] prefix.
 * <p>
 * For unordered indexes, there is no order defined for index keys. The only
 * comparison operation supported is the full equality of keys expressed as
 * composite values. Partial key matching is impossible.
 */
public final class CompositeValue implements Comparable<CompositeValue>, IdentifiedDataSerializable {

    /**
     * Represents a value that is equal to itself and less than any other value.
     */
    public static final ComparableIdentifiedDataSerializable NEGATIVE_INFINITY = new NegativeInfinity();

    /**
     * Represents a value that is equal to itself and greater than any other value.
     */
    public static final ComparableIdentifiedDataSerializable POSITIVE_INFINITY = new PositiveInfinity();

    private static final int BYTE_MASK = 0xFF;

    private Comparable[] components;

    /**
     * Constructs a new composite value for the deserialization purposes.
     */
    public CompositeValue() {
    }

    /**
     * Constructs a new composite value from the given components.
     * <p>
     * For performance reasons, the ownership of the passed components array
     * is transferred to the newly constructed composite value.
     */
    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public CompositeValue(Comparable[] components) {
        this.components = components;
    }

    /**
     * Constructs a new composite value of the given width having the given
     * prefix value as its first component and the filler value as its
     * remaining components.
     */
    public CompositeValue(int width, Comparable prefix, Comparable filler) {
        assert width > 0;
        Comparable[] components = new Comparable[width];
        components[0] = prefix;
        Arrays.fill(components, 1, components.length, filler);
        this.components = components;
    }

    /**
     * Returns the components of this composite value.
     * <p>
     * For performance reasons, the internal components array is directly
     * exposed to the caller.
     */
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public Comparable[] getComponents() {
        return components;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(components);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompositeValue that = (CompositeValue) o;

        // We are not using Comparables.equal here since an externally
        // canonicalized composite value is expected here, otherwise we can't
        // guarantee hashCode is working properly.
        return Arrays.equals(components, that.components);
    }

    @Override
    public String toString() {
        return Arrays.toString(components);
    }

    @SuppressFBWarnings("EQ_COMPARETO_USE_OBJECT_EQUALS")
    @Override
    public int compareTo(CompositeValue that) {
        assert components.length == that.components.length;

        for (int i = 0; i < components.length; ++i) {
            int order = compareComponent(components[i], that.components[i]);
            if (order != 0) {
                return order;
            }
        }

        return 0;
    }

    @Override
    public int getFactoryId() {
        return PredicateDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return PredicateDataSerializerHook.COMPOSITE_VALUE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(components.length);
        for (Comparable component : components) {
            out.writeObject(component);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int length = in.readByte() & BYTE_MASK;
        components = new Comparable[length];
        for (int i = 0; i < length; ++i) {
            components[i] = in.readObject();
        }
    }

    @SuppressWarnings("unchecked")
    private static int compareComponent(Comparable lhs, Comparable rhs) {
        if (rhs == NULL || rhs == NEGATIVE_INFINITY || rhs == POSITIVE_INFINITY) {
            return -signum(rhs.compareTo(lhs));
        } else {
            return Comparables.compare(lhs, rhs);
        }
    }

    private static final class NegativeInfinity implements ComparableIdentifiedDataSerializable {

        private NegativeInfinity() {
        }

        @Override
        public int getFactoryId() {
            return PredicateDataSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return PredicateDataSerializerHook.NEGATIVE_INFINITY;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
            // nothing to write
        }

        @Override
        public void readData(ObjectDataInput in) {
            // nothing to read
        }

        @SuppressFBWarnings("EQ_COMPARETO_USE_OBJECT_EQUALS")
        @SuppressWarnings("NullableProblems")
        @Override
        public int compareTo(Object o) {
            return o == this ? 0 : -1;
        }

        @Override
        public String toString() {
            return "-INF";
        }

    }

    private static final class PositiveInfinity implements ComparableIdentifiedDataSerializable {

        private PositiveInfinity() {
        }

        @Override
        public int getFactoryId() {
            return PredicateDataSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return PredicateDataSerializerHook.POSITIVE_INFINITY;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
            // nothing to write
        }

        @Override
        public void readData(ObjectDataInput in) {
            // nothing to read
        }

        @SuppressFBWarnings("EQ_COMPARETO_USE_OBJECT_EQUALS")
        @SuppressWarnings("NullableProblems")
        @Override
        public int compareTo(Object o) {
            return o == this ? 0 : +1;
        }

        @Override
        public String toString() {
            return "+INF";
        }

    }

}
