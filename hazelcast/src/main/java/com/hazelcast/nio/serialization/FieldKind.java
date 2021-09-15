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

package com.hazelcast.nio.serialization;

import com.hazelcast.spi.annotation.Beta;

/**
 * FieldKind for Compact {@link com.hazelcast.config.CompactSerializationConfig} and {@link Portable} formats.
 * It is designed to be used with {@link GenericRecord#getFieldKind(String)} API.
 * <p>
 * Note that actual id's in {@link FieldType} and {@link FieldKind} are not matching.
 * {@link FieldType} is the old API for Portable only and only meant to be used with
 * {@link ClassDefinition#getFieldType(String)} API.
 *
 * @since Hazelcast 5.0 as BETA. The final version will not be backward compatible with the Beta.
 */
@Beta
public enum FieldKind {

    BOOLEAN(0),
    BOOLEAN_ARRAY(1),
    BYTE(2),
    BYTE_ARRAY(3),
    CHAR(4),
    CHAR_ARRAY(5),
    SHORT(6),
    SHORT_ARRAY(7),
    INT(8),
    INT_ARRAY(9),
    LONG(10),
    LONG_ARRAY(11),
    FLOAT(12),
    FLOAT_ARRAY(13),
    DOUBLE(14),
    DOUBLE_ARRAY(15),
    STRING(16),
    STRING_ARRAY(17),
    DECIMAL(18),
    DECIMAL_ARRAY(19),
    TIME(20),
    TIME_ARRAY(21),
    DATE(22),
    DATE_ARRAY(23),
    TIMESTAMP(24),
    TIMESTAMP_ARRAY(25),
    TIMESTAMP_WITH_TIMEZONE(26),
    TIMESTAMP_WITH_TIMEZONE_ARRAY(27),
    COMPACT(28),
    COMPACT_ARRAY(29),
    PORTABLE(30),
    PORTABLE_ARRAY(31);

    private static final FieldKind[] ALL = FieldKind.values();
    private final int id;

    FieldKind(int id) {
        this.id = id;
    }

    public static FieldKind get(int type) {
        return ALL[type];
    }

    public int getId() {
        return id;
    }
}
