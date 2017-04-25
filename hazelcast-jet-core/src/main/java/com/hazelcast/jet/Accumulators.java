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
 * Collection of mutable primitive types and values.
 */
public final class Accumulators {

    private Accumulators() {
    }

    /**
     * Mutable {@code int} container.
     */
    public static class MutableInteger {

        /**
         * Internal value.
         */
        @SuppressWarnings("checkstyle:visibilitymodifier")
        public int value;

        /**
         * Create new instance with {@code value == 0}.
         */
        public MutableInteger() {
        }

        /**
         * Create new instance with specified value.
         */
        public MutableInteger(int value) {
            this.value = value;
        }

        /**
         * Get the value
         */
        public int getValue() {
            return value;
        }

        /**
         * Set the value
         */
        public void setValue(int value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MutableInteger that = (MutableInteger) o;
            return value == that.value;
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
     * Mutable {@code long} container.
     */
    public static class MutableLong {

        /**
         * Internal value.
         */
        @SuppressWarnings("checkstyle:visibilitymodifier")
        public long value;

        /**
         * Create new instance with {@code value == 0}.
         */
        public MutableLong() {
        }

        /**
         * Create new instance with specified value.
         */
        public MutableLong(long value) {
            this.value = value;
        }

        /**
         * Get the value
         */
        public long getValue() {
            return value;
        }

        /**
         * Set the value
         */
        public void setValue(long value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MutableLong that = (MutableLong) o;
            return value == that.value;
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
         * Internal value.
         */
        @SuppressWarnings("checkstyle:visibilitymodifier")
        public double value;

        /**
         * Create new instance with {@code value == 0}.
         */
        public MutableDouble() {
        }

        /**
         * Create new instance with specified value.
         */
        public MutableDouble(double value) {
            this.value = value;
        }

        /**
         * Get the value
         */
        public double getValue() {
            return value;
        }

        /**
         * Set the value
         */
        public void setValue(double value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MutableDouble that = (MutableDouble) o;
            return value == that.value;
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
     * Mutable object container.
     *
     * @param <T> referenced object type
     */
    public static class MutableReference<T> {

        /**
         * Internal value.
         */
        @SuppressWarnings("checkstyle:visibilitymodifier")
        public T value;

        /**
         * Create new instance with {@code null} value.
         */
        public MutableReference() {
        }

        /**
         * Create new instance with specified value.
         */
        public MutableReference(T value) {
            this.value = value;
        }

        /**
         * Get the value
         */
        public T getValue() {
            return value;
        }

        /**
         * Set the value
         */
        public void setValue(T value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return Objects.equals(value, ((MutableReference<?>) o).value);
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
