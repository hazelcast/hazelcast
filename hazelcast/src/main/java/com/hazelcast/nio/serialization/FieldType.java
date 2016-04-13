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

package com.hazelcast.nio.serialization;

import static com.hazelcast.nio.Bits.BOOLEAN_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.DOUBLE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.FLOAT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

public enum FieldType {

    // SINGLE-VALUE TYPES
    PORTABLE(0, Integer.MAX_VALUE),
    BYTE(1, BYTE_SIZE_IN_BYTES),
    BOOLEAN(2, BOOLEAN_SIZE_IN_BYTES),
    CHAR(3, CHAR_SIZE_IN_BYTES),
    SHORT(4, SHORT_SIZE_IN_BYTES),
    INT(5, INT_SIZE_IN_BYTES),
    LONG(6, LONG_SIZE_IN_BYTES),
    FLOAT(7, FLOAT_SIZE_IN_BYTES),
    DOUBLE(8, DOUBLE_SIZE_IN_BYTES),
    UTF(9, Integer.MAX_VALUE),

    // ARRAY TYPES
    PORTABLE_ARRAY(10, Integer.MAX_VALUE),
    BYTE_ARRAY(11, Integer.MAX_VALUE),
    BOOLEAN_ARRAY(12, Integer.MAX_VALUE),
    CHAR_ARRAY(13, Integer.MAX_VALUE),
    SHORT_ARRAY(14, Integer.MAX_VALUE),
    INT_ARRAY(15, Integer.MAX_VALUE),
    LONG_ARRAY(16, Integer.MAX_VALUE),
    FLOAT_ARRAY(17, Integer.MAX_VALUE),
    DOUBLE_ARRAY(18, Integer.MAX_VALUE),
    UTF_ARRAY(19, Integer.MAX_VALUE);

    private static final FieldType[] ALL = FieldType.values();
    private static final int VAR_SIZE = Integer.MAX_VALUE;

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
        return type >= PORTABLE_ARRAY.type;
    }

    public FieldType getSingleType() {
        if (isArrayType()) {
            return get((byte) (getId() % 10));
        }
        return this;
    }

    public FieldType getArrayType() {
        if (isArrayType()) {
            return this;
        }
        return get((byte) (getId() + 10));
    }

    public int getSingleElementSize() {
        int elementSize = getSingleType().elementSize;
        if (elementSize == VAR_SIZE) {
            throw new IllegalArgumentException("Unsupported type - the size is variable!");
        }
        return elementSize;
    }

}
