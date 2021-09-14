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

    BOOLEAN,
    BOOLEAN_ARRAY,
    BYTE,
    BYTE_ARRAY,
    CHAR,
    CHAR_ARRAY,
    SHORT,
    SHORT_ARRAY,
    INT,
    INT_ARRAY,
    LONG,
    LONG_ARRAY,
    FLOAT,
    FLOAT_ARRAY,
    DOUBLE,
    DOUBLE_ARRAY,
    STRING,
    STRING_ARRAY,
    DECIMAL,
    DECIMAL_ARRAY,
    TIME,
    TIME_ARRAY,
    DATE,
    DATE_ARRAY,
    TIMESTAMP,
    TIMESTAMP_ARRAY,
    TIMESTAMP_WITH_TIMEZONE,
    TIMESTAMP_WITH_TIMEZONE_ARRAY,
    COMPACT,
    COMPACT_ARRAY,
    PORTABLE,
    PORTABLE_ARRAY;
}
