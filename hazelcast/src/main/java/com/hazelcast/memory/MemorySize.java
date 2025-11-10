/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.memory;

/**
 * MemorySize represents a memory size with given value and <code>{@link MemoryUnit}</code>.
 *
 * @see MemoryUnit
 * @see Capacity
 * @since 3.4
 *
 * @deprecated {@link Capacity} should be used instead.
 */
@Deprecated(since = "5.1")
public final class MemorySize extends Capacity {

    public MemorySize(long value) {
        super(value, MemoryUnit.BYTES);
    }

    public MemorySize(long value, MemoryUnit unit) {
        super(value, unit);
    }

    /**
     * Parses string representation of a memory size value.
     * Value may end with one of suffixes;
     * <ul>
     * <li>'k' or 'K' for 'kilo',</li>
     * <li>'m' or 'M' for 'mega',</li>
     * <li>'g' or 'G' for 'giga'.</li>
     * </ul>
     * <p>
     * Default unit is bytes.
     * <p>
     * Examples:
     * 12345, 12345m, 12345K, 123456G
     *
     * @deprecated use {@link Capacity#parse(String)} instead.
     */
    @Deprecated(since = "5.1")
    public static MemorySize parse(String value) {
        return parse(value, MemoryUnit.BYTES);
    }

    /**
     * Parses string representation of a memory size value.
     * Value may end with one of suffixes;
     * <ul>
     * <li>'k' or 'K' for 'kilo',</li>
     * <li>'m' or 'M' for 'mega',</li>
     * <li>'g' or 'G' for 'giga'.</li>
     * </ul>
     * <p>
     * Uses default unit if value does not contain unit information.
     * <p>
     * Examples:
     * 12345, 12345m, 12345K, 123456G
     *
     * @deprecated use {@link Capacity#parse(String, MemoryUnit)} instead
     */
    @Deprecated(since = "5.1")
    public static MemorySize parse(String value, MemoryUnit defaultUnit) {
        if (value == null || value.length() == 0) {
            return new MemorySize(0, MemoryUnit.BYTES);
        }

        MemoryUnit unit = defaultUnit;
        final char last = value.charAt(value.length() - 1);
        if (!Character.isDigit(last)) {
            value = value.substring(0, value.length() - 1);
            switch (last) {
                case 'g':
                case 'G':
                    unit = MemoryUnit.GIGABYTES;
                    break;

                case 'm':
                case 'M':
                    unit = MemoryUnit.MEGABYTES;
                    break;

                case 'k':
                case 'K':
                    unit = MemoryUnit.KILOBYTES;
                    break;

                default:
                    throw new IllegalArgumentException("Could not determine memory unit of " + value + last);
            }
        }

        return new MemorySize(Long.parseLong(value), unit);
    }
}
