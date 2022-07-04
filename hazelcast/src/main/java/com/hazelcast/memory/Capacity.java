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

package com.hazelcast.memory;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Capacity represents a memory capacity with given value and <code>{@link MemoryUnit}</code>.
 * This class is immutable.
 *
 * @see MemoryUnit
 * @since 5.1
 */
public class Capacity {
    private static final int PRETTY_FORMAT_LIMIT = 10;

    private final long value;
    private final MemoryUnit unit;

    public Capacity(long value) {
        this(value, MemoryUnit.BYTES);
    }

    public Capacity(long value, MemoryUnit unit) {
        if (value < 0) {
            throw new IllegalArgumentException("Capacity cannot be negative! -> " + value);
        }

        this.value = value;
        this.unit = checkNotNull(unit, "MemoryUnit is required!");
    }

    /**
     * Returns value of the capacity in its original unit.
     *
     * @return capacity in its original unit
     */
    public long getValue() {
        return value;
    }

    /**
     * Returns unit of the capacity
     *
     * @return unit of capacity
     */
    public MemoryUnit getUnit() {
        return unit;
    }

    /**
     * Returns value of the capacity in bytes.
     *
     * @return capacity in bytes
     */
    public long bytes() {
        return unit.toBytes(value);
    }

    /**
     * Returns value of the capacity in kilo-bytes.
     *
     * @return capacity in kilo-bytes
     */
    public long kiloBytes() {
        return unit.toKiloBytes(value);
    }

    /**
     * Returns value of the capacity in mega-bytes.
     *
     * @return capacity in mega-bytes
     */
    public long megaBytes() {
        return unit.toMegaBytes(value);
    }

    /**
     * Returns value of the capacity in giga-bytes.
     *
     * @return capacity in giga-bytes
     */
    public long gigaBytes() {
        return unit.toGigaBytes(value);
    }

    /**
     * Returns an instance of {@link Capacity}.
     *
     * @param value value of the capacity
     * @param unit unit of the capacity
     * @return a {@link Capacity} instance
     */
    public static Capacity of(long value, MemoryUnit unit) {
        return new Capacity(value, unit);
    }

    /**
     * Parses string representation of a capacity.
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
     */
    public static Capacity parse(String value) {
        return parse(value, MemoryUnit.BYTES);
    }

    /**
     * Parses string representation of a capacity.
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
     */
    public static Capacity parse(String value, MemoryUnit defaultUnit) {
        if (value == null || value.length() == 0) {
            return new Capacity(0, MemoryUnit.BYTES);
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

        return new Capacity(Long.parseLong(value), unit);
    }

    /**
     * Returns a pretty format String representation of this capacity.
     *
     * @return a pretty format representation of this capacity
     */
    public String toPrettyString() {
        return toPrettyString(value, unit);
    }

    @Override
    public final String toString() {
        return value + " " + unit.toString();
    }

    /**
     * Utility method to create a pretty format representation of given capacity in bytes.
     *
     * @param capacity capacity in bytes
     * @return pretty format representation of given capacity
     */
    public static String toPrettyString(long capacity) {
        return toPrettyString(capacity, MemoryUnit.BYTES);
    }

    /**
     * Utility method to create a pretty format representation of given capacity with a specified unit.
     *
     * @param capacity capacity
     * @param unit unit
     * @return pretty format representation of given capacity
     */
    public static String toPrettyString(long capacity, MemoryUnit unit) {
        if (unit.toGigaBytes(capacity) >= PRETTY_FORMAT_LIMIT) {
            return unit.toGigaBytes(capacity) + " GB";
        }
        if (unit.toMegaBytes(capacity) >= PRETTY_FORMAT_LIMIT) {
            return unit.toMegaBytes(capacity) + " MB";
        }
        if (unit.toKiloBytes(capacity) >= PRETTY_FORMAT_LIMIT) {
            return unit.toKiloBytes(capacity) + " KB";
        }
        if (capacity % MemoryUnit.K == 0) {
            return unit.toKiloBytes(capacity) + " KB";
        }
        return capacity + " bytes";
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Capacity)) {
            return false;
        }

        Capacity that = (Capacity) o;

        if (value != that.value) {
            return false;
        }
        return unit == that.unit;
    }

    @Override
    public final int hashCode() {
        int result = (int) (value ^ (value >>> 32));
        result = 31 * result + (unit != null ? unit.hashCode() : 0);
        return result;
    }
}
