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

import com.hazelcast.nio.serialization.FieldID;
import com.hazelcast.nio.serialization.FieldType;

import javax.annotation.Nonnull;

/**
 * Utility class to convert field type to field ID.
 */
@SuppressWarnings({"checkstyle:ReturnCount", "MethodLength", "CyclomaticComplexity"})
public final class FieldTypeToFieldID {

    private FieldTypeToFieldID() {
    }

    @Nonnull
    public static FieldID toFieldID(@Nonnull FieldType fieldType) {
        switch (fieldType) {
            case PORTABLE:
                return FieldID.PORTABLE;
            case BYTE:
                return FieldID.BYTE;
            case BOOLEAN:
                return FieldID.BOOLEAN;
            case CHAR:
                return FieldID.CHAR;
            case SHORT:
                return FieldID.SHORT;
            case INT:
                return FieldID.INT;
            case LONG:
                return FieldID.LONG;
            case FLOAT:
                return FieldID.FLOAT;
            case DOUBLE:
                return FieldID.DOUBLE;
            case UTF:
                return FieldID.STRING;
            case PORTABLE_ARRAY:
                return FieldID.PORTABLE_ARRAY;
            case BYTE_ARRAY:
                return FieldID.BYTE_ARRAY;
            case BOOLEAN_ARRAY:
                return FieldID.BOOLEAN_ARRAY;
            case CHAR_ARRAY:
                return FieldID.CHAR_ARRAY;
            case SHORT_ARRAY:
                return FieldID.SHORT_ARRAY;
            case INT_ARRAY:
                return FieldID.INT_ARRAY;
            case LONG_ARRAY:
                return FieldID.LONG_ARRAY;
            case FLOAT_ARRAY:
                return FieldID.FLOAT_ARRAY;
            case DOUBLE_ARRAY:
                return FieldID.DOUBLE_ARRAY;
            case UTF_ARRAY:
                return FieldID.STRING_ARRAY;
            case DECIMAL:
                return FieldID.DECIMAL;
            case DECIMAL_ARRAY:
                return FieldID.DECIMAL_ARRAY;
            case TIME:
                return FieldID.TIME;
            case TIME_ARRAY:
                return FieldID.TIME_ARRAY;
            case DATE:
                return FieldID.DATE;
            case DATE_ARRAY:
                return FieldID.DATE_ARRAY;
            case TIMESTAMP:
                return FieldID.TIMESTAMP;
            case TIMESTAMP_ARRAY:
                return FieldID.TIMESTAMP_ARRAY;
            case TIMESTAMP_WITH_TIMEZONE:
                return FieldID.TIMESTAMP_WITH_TIMEZONE;
            case TIMESTAMP_WITH_TIMEZONE_ARRAY:
                return FieldID.TIMESTAMP_WITH_TIMEZONE_ARRAY;
            default:
                //This statement will never be called.
                throw new IllegalStateException("There is no corresponding field id for given field type : " + fieldType);
        }
    }
}
