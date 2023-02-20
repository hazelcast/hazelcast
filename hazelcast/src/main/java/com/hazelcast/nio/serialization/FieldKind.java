/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;

/**
 * FieldKind for {@link com.hazelcast.config.CompactSerializationConfig Compact} and {@link Portable} formats.
 * It is designed to be used with {@link GenericRecord#getFieldKind(String)} API.
 * <p>
 * Note that actual id's in {@link FieldType} and {@link FieldKind} are not matching.
 * {@link FieldType} is the old API for Portable only and only meant to be used with
 * {@link ClassDefinition#getFieldType(String)} API.
 *
 * @since Hazelcast 5.2
 */
public enum FieldKind {

    /**
     * Represents fields that do not exist.
     */
    NOT_AVAILABLE(0),
    BOOLEAN(1),
    ARRAY_OF_BOOLEAN(2),
    INT8(3),
    ARRAY_OF_INT8(4),

    /**
     * Supported only for {@link Portable}. Not applicable to {@link com.hazelcast.config.CompactSerializationConfig Compact}
     */
    CHAR(5),

    /**
     * Supported only for {@link Portable}. Not applicable to {@link com.hazelcast.config.CompactSerializationConfig Compact}
     */
    ARRAY_OF_CHAR(6),
    INT16(7),
    ARRAY_OF_INT16(8),
    INT32(9),
    ARRAY_OF_INT32(10),
    INT64(11),
    ARRAY_OF_INT64(12),
    FLOAT32(13),
    ARRAY_OF_FLOAT32(14),
    FLOAT64(15),
    ARRAY_OF_FLOAT64(16),
    STRING(17),
    ARRAY_OF_STRING(18),
    DECIMAL(19),
    ARRAY_OF_DECIMAL(20),
    TIME(21),
    ARRAY_OF_TIME(22),
    DATE(23),
    ARRAY_OF_DATE(24),
    TIMESTAMP(25),
    ARRAY_OF_TIMESTAMP(26),
    TIMESTAMP_WITH_TIMEZONE(27),
    ARRAY_OF_TIMESTAMP_WITH_TIMEZONE(28),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    COMPACT(29),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    ARRAY_OF_COMPACT(30),

    /**
     * Supported only for {@link Portable}. Not applicable to {@link com.hazelcast.config.CompactSerializationConfig Compact}
     */
    PORTABLE(31),

    /**
     * Supported only for {@link Portable}. Not applicable to Compact{@link com.hazelcast.config.CompactSerializationConfig}
     */
    ARRAY_OF_PORTABLE(32),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    NULLABLE_BOOLEAN(33),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    ARRAY_OF_NULLABLE_BOOLEAN(34),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    NULLABLE_INT8(35),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    ARRAY_OF_NULLABLE_INT8(36),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    NULLABLE_INT16(37),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    ARRAY_OF_NULLABLE_INT16(38),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    NULLABLE_INT32(39),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    ARRAY_OF_NULLABLE_INT32(40),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    NULLABLE_INT64(41),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    ARRAY_OF_NULLABLE_INT64(42),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    NULLABLE_FLOAT32(43),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    ARRAY_OF_NULLABLE_FLOAT32(44),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    NULLABLE_FLOAT64(45),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    ARRAY_OF_NULLABLE_FLOAT64(46);

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
