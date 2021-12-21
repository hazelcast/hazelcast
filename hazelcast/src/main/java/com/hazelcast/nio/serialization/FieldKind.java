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
    ARRAY_OF_BOOLEANS(1),
    INT8(2),
    ARRAY_OF_INT8S(3),
    CHAR(4),
    ARRAY_OF_CHARS(5),
    INT16(6),
    ARRAY_OF_INT16S(7),
    INT32(8),
    ARRAY_OF_INT32S(9),
    INT64(10),
    ARRAY_OF_INT64S(11),
    FLOAT32(12),
    ARRAY_OF_FLOAT32S(13),
    FLOAT64(14),
    ARRAY_OF_FLOAT64S(15),
    STRING(16),
    ARRAY_OF_STRINGS(17),
    DECIMAL(18),
    ARRAY_OF_DECIMALS(19),
    TIME(20),
    ARRAY_OF_TIMES(21),
    DATE(22),
    ARRAY_OF_DATES(23),
    TIMESTAMP(24),
    ARRAY_OF_TIMESTAMPS(25),
    TIMESTAMP_WITH_TIMEZONE(26),
    ARRAY_OF_TIMESTAMP_WITH_TIMEZONES(27),
    COMPACT(28),
    ARRAY_OF_COMPACTS(29),
    PORTABLE(30),
    ARRAY_OF_PORTABLES(31),
    NULLABLE_BOOLEAN(32),
    ARRAY_OF_NULLABLE_BOOLEANS(33),
    NULLABLE_INT8(34),
    ARRAY_OF_NULLABLE_INT8S(35),
    NULLABLE_INT16(36),
    ARRAY_OF_NULLABLE_INT16S(37),
    NULLABLE_INT32(38),
    ARRAY_OF_NULLABLE_INT32S(39),
    NULLABLE_INT64(40),
    ARRAY_OF_NULLABLE_INT64S(41),
    NULLABLE_FLOAT32(42),
    ARRAY_OF_NULLABLE_FLOAT32S(43),
    NULLABLE_FLOAT64(44),
    ARRAY_OF_NULLABLE_FLOAT64S(45);

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
