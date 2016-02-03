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

public enum FieldType {

    PORTABLE(0),
    BYTE(1),
    BOOLEAN(2),
    CHAR(3),
    SHORT(4),
    INT(5),
    LONG(6),
    FLOAT(7),
    DOUBLE(8),
    UTF(9),
    PORTABLE_ARRAY(10),
    BYTE_ARRAY(11),
    BOOLEAN_ARRAY(12),
    CHAR_ARRAY(13),
    SHORT_ARRAY(14),
    INT_ARRAY(15),
    LONG_ARRAY(16),
    FLOAT_ARRAY(17),
    DOUBLE_ARRAY(18),
    UTF_ARRAY(19);

    private static final FieldType[] ALL = FieldType.values();

    private final byte type;

    FieldType(int type) {
        this.type = (byte) type;
    }

    public byte getId() {
        return type;
    }

    public static FieldType get(byte type) {
        return ALL[type];
    }

}
