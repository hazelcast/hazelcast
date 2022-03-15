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

package com.hazelcast.nio.serialization;

import static com.hazelcast.internal.nio.Bits.BOOLEAN_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.DOUBLE_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.FLOAT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.SHORT_SIZE_IN_BYTES;
import static java.lang.Integer.MAX_VALUE;

/**
 * Field Type for {@link Portable} format to be used with {@link ClassDefinition#getFieldType(String)} API
 */
public enum FieldType {

    PORTABLE(0, MAX_VALUE),
    BYTE(1, BYTE_SIZE_IN_BYTES),
    BOOLEAN(2, BOOLEAN_SIZE_IN_BYTES),
    CHAR(3, CHAR_SIZE_IN_BYTES),
    SHORT(4, SHORT_SIZE_IN_BYTES),
    INT(5, INT_SIZE_IN_BYTES),
    LONG(6, LONG_SIZE_IN_BYTES),
    FLOAT(7, FLOAT_SIZE_IN_BYTES),
    DOUBLE(8, DOUBLE_SIZE_IN_BYTES),
    UTF(9, MAX_VALUE),
    PORTABLE_ARRAY(10, MAX_VALUE),
    BYTE_ARRAY(11, MAX_VALUE),
    BOOLEAN_ARRAY(12, MAX_VALUE),
    CHAR_ARRAY(13, MAX_VALUE),
    SHORT_ARRAY(14, MAX_VALUE),
    INT_ARRAY(15, MAX_VALUE),
    LONG_ARRAY(16, MAX_VALUE),
    FLOAT_ARRAY(17, MAX_VALUE),
    DOUBLE_ARRAY(18, MAX_VALUE),
    UTF_ARRAY(19, MAX_VALUE),

    DECIMAL(20, MAX_VALUE),
    DECIMAL_ARRAY(21, MAX_VALUE),
    TIME(22, BYTE_SIZE_IN_BYTES * 3 + INT_SIZE_IN_BYTES),
    TIME_ARRAY(23, MAX_VALUE),
    DATE(24, SHORT_SIZE_IN_BYTES + BYTE_SIZE_IN_BYTES * 2),
    DATE_ARRAY(25, MAX_VALUE),
    TIMESTAMP(26, TIME.getTypeSize() + DATE.getTypeSize()),
    TIMESTAMP_ARRAY(27, MAX_VALUE),
    TIMESTAMP_WITH_TIMEZONE(28, TIMESTAMP.getTypeSize() + INT_SIZE_IN_BYTES),
    TIMESTAMP_WITH_TIMEZONE_ARRAY(29, MAX_VALUE);

    private static final FieldType[] ALL = FieldType.values();
    private static final int TYPES_COUNT = 10;

    private final byte type;
    private final int elementSize;

    FieldType(int type, int elementSize) {
        this.type = (byte) type;
        this.elementSize = elementSize;
    }

    public byte getId() {
        return type;
    }

    public static FieldType get(byte type) {
        return ALL[type];
    }

    public boolean isArrayType() {
        if (type < DECIMAL.type) {
            return type >= PORTABLE_ARRAY.type;
        }
        return type % 2 != 0;
    }

    public FieldType getSingleType() {
        byte id = getId();
        if (type < DECIMAL.type) {
            return get((byte) (id % TYPES_COUNT));
        }
        if (id % 2 == 0) {
            return get(id);
        } else {
            return get((byte) (id - 1));
        }
    }

    public boolean hasDefiniteSize() {
        return elementSize != MAX_VALUE;
    }

    /**
     * @return size of an element of the type represented by this object
     * @throws IllegalArgumentException if the type does not have a definite size.
     *                                  Invoke {@link #hasDefiniteSize()} to check first.
     */
    public int getTypeSize() throws IllegalArgumentException {
        if (elementSize == MAX_VALUE) {
            // unknown size case
            throw new IllegalArgumentException("Unsupported type - the size is variable or unknown");
        }
        return elementSize;
    }

}
