/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

/**
 * Type of Attribute
 */
public enum AttributeType {
    /**
     * Double
     */
    DOUBLE(TypeConverters.DOUBLE_CONVERTER),
    /**
     * Long
     */
    LONG(TypeConverters.LONG_CONVERTER),
    /**
     * Short
     */
    SHORT(TypeConverters.SHORT_CONVERTER),
    /**
     * Boolean
     */
    BOOLEAN(TypeConverters.BOOLEAN_CONVERTER),
    /**
     * Byte
     */
    BYTE(TypeConverters.BYTE_CONVERTER),
    /**
     * String
     */
    STRING(TypeConverters.STRING_CONVERTER),
    /**
     * Float
     */
    FLOAT(TypeConverters.FLOAT_CONVERTER),
    /**
     * Char
     */
    CHAR(TypeConverters.CHAR_CONVERTER),
    /**
     * Integer
     */
    INTEGER(TypeConverters.INTEGER_CONVERTER),
    /**
     * Enum
     */
    ENUM(TypeConverters.ENUM_CONVERTER),
    /**
     * Big Integer
     */
    BIG_INTEGER(TypeConverters.BIG_INTEGER_CONVERTER),
    /**
     * Big Decimal
     */
    BIG_DECIMAL(TypeConverters.BIG_DECIMAL_CONVERTER),
    /**
     * Sql Time Stamp
     */
    SQL_TIMESTAMP(TypeConverters.SQL_TIMESTAMP_CONVERTER),
    /**
     * Sql Date
     */
    SQL_DATE(TypeConverters.SQL_DATE_CONVERTER),
    /**
     * Date
     */
    DATE(TypeConverters.DATE_CONVERTER),
    /**
     * UUID
     */
    UUID(UUIDConverter.INSTANCE);

    private final TypeConverters.TypeConverter converter;

    private AttributeType(TypeConverters.TypeConverter converter) {
        this.converter = converter;
    }

    public TypeConverters.TypeConverter getConverter() {
        return converter;
    }

}
