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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.FieldType;

import javax.annotation.Nonnull;

/**
 * Utility class to convert FieldType to FieldKind.
 */
@SuppressWarnings({"checkstyle:ReturnCount", "MethodLength", "CyclomaticComplexity"})
public final class FieldTypeToFieldKind {

    private FieldTypeToFieldKind() {
    }

    @Nonnull
    public static FieldKind toFieldKind(@Nonnull FieldType fieldType) {
        switch (fieldType) {
            case PORTABLE:
                return FieldKind.PORTABLE;
            case BYTE:
                return FieldKind.BYTE;
            case BOOLEAN:
                return FieldKind.BOOLEAN;
            case CHAR:
                return FieldKind.CHAR;
            case SHORT:
                return FieldKind.SHORT;
            case INT:
                return FieldKind.INT;
            case LONG:
                return FieldKind.LONG;
            case FLOAT:
                return FieldKind.FLOAT;
            case DOUBLE:
                return FieldKind.DOUBLE;
            case UTF:
                return FieldKind.STRING;
            case PORTABLE_ARRAY:
                return FieldKind.PORTABLE_ARRAY;
            case BYTE_ARRAY:
                return FieldKind.BYTE_ARRAY;
            case BOOLEAN_ARRAY:
                return FieldKind.BOOLEAN_ARRAY;
            case CHAR_ARRAY:
                return FieldKind.CHAR_ARRAY;
            case SHORT_ARRAY:
                return FieldKind.SHORT_ARRAY;
            case INT_ARRAY:
                return FieldKind.INT_ARRAY;
            case LONG_ARRAY:
                return FieldKind.LONG_ARRAY;
            case FLOAT_ARRAY:
                return FieldKind.FLOAT_ARRAY;
            case DOUBLE_ARRAY:
                return FieldKind.DOUBLE_ARRAY;
            case UTF_ARRAY:
                return FieldKind.STRING_ARRAY;
            case DECIMAL:
                return FieldKind.DECIMAL;
            case DECIMAL_ARRAY:
                return FieldKind.DECIMAL_ARRAY;
            case TIME:
                return FieldKind.TIME;
            case TIME_ARRAY:
                return FieldKind.TIME_ARRAY;
            case DATE:
                return FieldKind.DATE;
            case DATE_ARRAY:
                return FieldKind.DATE_ARRAY;
            case TIMESTAMP:
                return FieldKind.TIMESTAMP;
            case TIMESTAMP_ARRAY:
                return FieldKind.TIMESTAMP_ARRAY;
            case TIMESTAMP_WITH_TIMEZONE:
                return FieldKind.TIMESTAMP_WITH_TIMEZONE;
            case TIMESTAMP_WITH_TIMEZONE_ARRAY:
                return FieldKind.TIMESTAMP_WITH_TIMEZONE_ARRAY;
            default:
                //This statement will never be called.
                throw new IllegalStateException("There is no corresponding field id for given field type : " + fieldType);
        }
    }
}
