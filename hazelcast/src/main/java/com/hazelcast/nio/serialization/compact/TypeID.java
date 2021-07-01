/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.nio.serialization.compact;

/**
 * Type ID's of Compact format which corresponds to methods on
 * {@link CompactReader}
 * {@link CompactWriter}
 * {@link CompactRecord}
 * {@link CompactRecordBuilder}
 */
public enum TypeID {

    COMPOSED(0),
    COMPOSED_ARRAY(1),
    BOOLEAN(2, 1),
    BOOLEAN_ARRAY(3),
    BYTE(4, Byte.SIZE),
    BYTE_ARRAY(5),
    CHAR(6, Character.SIZE),
    CHAR_ARRAY(7),
    SHORT(8, Short.SIZE),
    SHORT_ARRAY(9),
    INT(10, Integer.SIZE),
    INT_ARRAY(11),
    FLOAT(12, Float.SIZE),
    FLOAT_ARRAY(13),
    LONG(14, Long.SIZE),
    LONG_ARRAY(15),
    DOUBLE(16, Double.SIZE),
    DOUBLE_ARRAY(17),
    STRING(18),
    STRING_ARRAY(19),
    DECIMAL(20),
    DECIMAL_ARRAY(21),
    TIME(22, Byte.SIZE * 3 + Integer.SIZE),
    TIME_ARRAY(23),
    DATE(24, Integer.SIZE + Byte.SIZE * 2),
    DATE_ARRAY(25),
    TIMESTAMP(26, TIME.getTypeSize() + DATE.getTypeSize()),
    TIMESTAMP_ARRAY(27),
    TIMESTAMP_WITH_TIMEZONE(28, TIME.getTypeSize() + DATE.getTypeSize() + Integer.SIZE),
    TIMESTAMP_WITH_TIMEZONE_ARRAY(29);

    private static final TypeID[] ALL = TypeID.values();

    private static final int VARIABLE_SIZE = -1;
    private final byte type;
    private final int elementSize;

    TypeID(int type, int elementSize) {
        this.type = (byte) type;
        this.elementSize = elementSize;
    }

    TypeID(int type) {
        this.type = (byte) type;
        this.elementSize = VARIABLE_SIZE;
    }

    public byte getId() {
        return type;
    }

    public static TypeID get(byte type) {
        return ALL[type];
    }

    public boolean isArrayType() {
        return type % 2 != 0;
    }

    public TypeID getSingleType() {
        if (isArrayType()) {
            return get((byte) (type - 1));
        } else {
            return get(type);
        }
    }

    public boolean hasDefiniteSize() {
        return elementSize != VARIABLE_SIZE;
    }

    /**
     * @return size of an element of the type represented by this object
     * @throws IllegalArgumentException if the type does not have a definite size.
     *                                  Invoke {@link #hasDefiniteSize()} to check first.
     */
    public int getTypeSize() throws IllegalArgumentException {
        if (elementSize == VARIABLE_SIZE) {
            // unknown size case
            throw new IllegalArgumentException("Unsupported type - the size is variable or unknown");
        }
        return elementSize;
    }
}
