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

package com.hazelcast.config.properties;

import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.util.Preconditions;

/**
 * This enum class contains basic {@link TypeConverter} implementations to
 * convert strings to all common basic Java types.
 */
public enum PropertyTypeConverter implements TypeConverter {
    /**
     * {@link TypeConverter} that bypasses a string
     */
    STRING {
        @Override
        public Comparable convert(Comparable value) {
            Preconditions.checkNotNull(value, "The value to convert cannot be null");
            return value.toString();
        }
    },

    /**
     * {@link TypeConverter} to convert an input string to an output short
     */
    SHORT {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof String) {
                return Short.parseShort((String) value);
            }
            throw new IllegalArgumentException("Cannot convert to short");
        }
    },

    /**
     * {@link TypeConverter} to convert an input string to an output int
     */
    INTEGER {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof String) {
                return Integer.parseInt((String) value);
            }
            throw new IllegalArgumentException("Cannot convert to integer");
        }
    },

    /**
     * {@link TypeConverter} to convert an input string to an output long
     */
    LONG {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof String) {
                return Long.parseLong((String) value);
            }
            throw new IllegalArgumentException("Cannot convert to long");
        }
    },

    /**
     * {@link TypeConverter} to convert an input string to an output float
     */
    FLOAT {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof String) {
                return Float.parseFloat((String) value);
            }
            throw new IllegalArgumentException("Cannot convert to float");
        }
    },

    /**
     * {@link TypeConverter} to convert an input string to an output double
     */
    DOUBLE {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof String) {
                return Double.parseDouble((String) value);
            }
            throw new IllegalArgumentException("Cannot convert to double");
        }
    },

    /**
     * {@link TypeConverter} to convert an input string to an output boolean
     */
    BOOLEAN {
        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof Boolean) {
                return value;
            } else if (value instanceof String) {
                return Boolean.parseBoolean((String) value);
            }
            throw new IllegalArgumentException("Cannot convert to boolean");
        }
    }
}
