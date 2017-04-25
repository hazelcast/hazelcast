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

package com.hazelcast.jet;

import java.util.Objects;

/**
 * Mutable holders of primitive values and references. The classes are
 * designed so that they have both getters/setters and the exposed value
 * field. Depending on context, one or the other is more convenient.
 */
public final class Accumulators {

    private Accumulators() {
    }

    /**
     * Mutable {@code int} holder.
     */
    public static class MutableInteger {

        /**
         * The holder's value.
         */
        @SuppressWarnings("checkstyle:visibilitymodifier")
        public int value;

        /**
         * Creates a new instance with {@code value == 0}.
         */
        public MutableInteger() {
        }

        /**
         * Creates new instance with the given initial value.
         */
        public MutableInteger(int value) {
            this.value = value;
        }

        /**
         * Returns the current value.
         */
        public int getValue() {
            return value;
        }

        /**
         * Sets the value as given.
         */
        public void setValue(int value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            return this == o ||
                    o != null
                    && this.getClass() == o.getClass()
                    && this.value == ((MutableInteger) o).value;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(value);
        }

        @Override
        public String toString() {
            return "MutableInteger(" + value + ')';
        }
    }


    /**
     * Mutable {@code long} holder.
     */
    public static class MutableLong {

        /**
         * The holder's value.
         */
        @SuppressWarnings("checkstyle:visibilitymodifier")
        public long value;

        /**
         * Creates a new instance with {@code value == 0}.
         */
        public MutableLong() {
        }

        /**
         * Creates a new instance with the specified value.
         */
        public MutableLong(long value) {
            this.value = value;
        }

        /**
         * Returns the current value.
         */
        public long getValue() {
            return value;
        }

        /**
         * Sets the value as given.
         */
        public void setValue(long value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            return this == o ||
                    o != null
                    && this.getClass() == o.getClass()
                    && this.value == ((MutableLong) o).value;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(value);
        }

        @Override
        public String toString() {
            return "MutableLong(" + value + ')';
        }
    }

    /**
     * Mutable {@code double} container.
     */
    public static class MutableDouble {

        /**
         * The holder's value.
         */
        @SuppressWarnings("checkstyle:visibilitymodifier")
        public double value;

        /**
         * Creates a new instance with {@code value == 0}.
         */
        public MutableDouble() {
        }

        /**
         * Creates a new instance with the specified value.
         */
        public MutableDouble(double value) {
            this.value = value;
        }

        /**
         * Returns the current value.
         */
        public double getValue() {
            return value;
        }

        /**
         * Sets the value as given.
         */
        public void setValue(double value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            return this == o ||
                    o != null
                    && this.getClass() == o.getClass()
                    && this.value == ((MutableDouble) o).value;
        }

        @Override
        public int hashCode() {
            return Double.hashCode(value);
        }

        @Override
        public String toString() {
            return "MutableDouble(" + value + ')';
        }
    }

    /**
     * Mutable container of an object reference.
     *
     * @param <T> referenced object type
     */
    public static class MutableReference<T> {

        /**
         * The holder's value.
         */
        @SuppressWarnings("checkstyle:visibilitymodifier")
        public T value;

        /**
         * Creates a new instance with a {@code null} value.
         */
        public MutableReference() {
        }

        /**
         * Creates a new instance with the specified value.
         */
        public MutableReference(T value) {
            this.value = value;
        }

        /**
         * Returns the current value.
         */
        public T getValue() {
            return value;
        }

        /**
         * Sets the value as given.
         */
        public void setValue(T value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            return this == o ||
                    o != null
                    && this.getClass() == o.getClass()
                    && Objects.equals(this.value, ((MutableReference) o).value);
        }

        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "MutableReference(" + value + ')';
        }
    }
}
