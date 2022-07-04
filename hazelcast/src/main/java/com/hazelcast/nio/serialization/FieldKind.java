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

import com.hazelcast.spi.annotation.Beta;

/**
 * FieldKind for {@link com.hazelcast.config.CompactSerializationConfig Compact} and {@link Portable} formats.
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
    ARRAY_OF_BOOLEAN(1),
    INT8(2),
    ARRAY_OF_INT8(3),

    /**
     * Supported only for {@link Portable}. Not applicable to {@link com.hazelcast.config.CompactSerializationConfig Compact}
     */
    CHAR(4),

    /**
     * Supported only for {@link Portable}. Not applicable to {@link com.hazelcast.config.CompactSerializationConfig Compact}
     */
    ARRAY_OF_CHAR(5),
    INT16(6),
    ARRAY_OF_INT16(7),
    INT32(8),
    ARRAY_OF_INT32(9),
    INT64(10),
    ARRAY_OF_INT64(11),
    FLOAT32(12),
    ARRAY_OF_FLOAT32(13),
    FLOAT64(14),
    ARRAY_OF_FLOAT64(15),
    STRING(16),
    ARRAY_OF_STRING(17),
    DECIMAL(18),
    ARRAY_OF_DECIMAL(19),
    TIME(20),
    ARRAY_OF_TIME(21),
    DATE(22),
    ARRAY_OF_DATE(23),
    TIMESTAMP(24),
    ARRAY_OF_TIMESTAMP(25),
    TIMESTAMP_WITH_TIMEZONE(26),
    ARRAY_OF_TIMESTAMP_WITH_TIMEZONE(27),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    COMPACT(28),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    ARRAY_OF_COMPACT(29),

    /**
     * Supported only for {@link Portable}. Not applicable to {@link com.hazelcast.config.CompactSerializationConfig Compact}
     */
    PORTABLE(30),

    /**
     * Supported only for {@link Portable}. Not applicable to Compact{@link com.hazelcast.config.CompactSerializationConfig}
     */
    ARRAY_OF_PORTABLE(31),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    NULLABLE_BOOLEAN(32),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    ARRAY_OF_NULLABLE_BOOLEAN(33),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    NULLABLE_INT8(34),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    ARRAY_OF_NULLABLE_INT8(35),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    NULLABLE_INT16(36),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    ARRAY_OF_NULLABLE_INT16(37),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    NULLABLE_INT32(38),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    ARRAY_OF_NULLABLE_INT32(39),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    NULLABLE_INT64(40),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    ARRAY_OF_NULLABLE_INT64(41),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    NULLABLE_FLOAT32(42),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    ARRAY_OF_NULLABLE_FLOAT32(43),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    NULLABLE_FLOAT64(44),

    /**
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     */
    ARRAY_OF_NULLABLE_FLOAT64(45);

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
