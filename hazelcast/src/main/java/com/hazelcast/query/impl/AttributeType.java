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

import com.hazelcast.nio.serialization.FieldType;

/**
 * Type of Attribute
 */
public enum AttributeType {
    /**
     * Double
     */
    DOUBLE(FieldType.DOUBLE.getId(), TypeConverters.DOUBLE_CONVERTER),
    /**
     * Long
     */
    LONG(FieldType.LONG.getId(), TypeConverters.LONG_CONVERTER),
    /**
     * Short
     */
    SHORT(FieldType.SHORT.getId(), TypeConverters.SHORT_CONVERTER),
    /**
     * Boolean
     */
    BOOLEAN(FieldType.BOOLEAN.getId(), TypeConverters.BOOLEAN_CONVERTER),
    /**
     * Byte
     */
    BYTE(FieldType.BYTE.getId(), TypeConverters.BYTE_CONVERTER),
    /**
     * String
     */
    STRING(FieldType.UTF.getId(), TypeConverters.STRING_CONVERTER),
    /**
     * Float
     */
    FLOAT(FieldType.FLOAT.getId(), TypeConverters.FLOAT_CONVERTER),
    /**
     * Char
     */
    CHAR(FieldType.CHAR.getId(), TypeConverters.CHAR_CONVERTER),
    /**
     * Integer
     */
    INTEGER(FieldType.INT.getId(), TypeConverters.INTEGER_CONVERTER),
    /**
     * Enum
     */
    ENUM(44, TypeConverters.ENUM_CONVERTER),
    /**
     * Big Integer
     */
    BIG_INTEGER(45, TypeConverters.BIG_INTEGER_CONVERTER),
    /**
     * Big Decimal
     */
    BIG_DECIMAL(46, TypeConverters.BIG_DECIMAL_CONVERTER),
    /**
     * Sql Time Stamp
     */
    SQL_TIMESTAMP(47, TypeConverters.SQL_TIMESTAMP_CONVERTER),
    /**
     * Sql Date
     */
    SQL_DATE(48, TypeConverters.SQL_DATE_CONVERTER),
    /**
     * Date
     */
    DATE(49, TypeConverters.DATE_CONVERTER);

    private static final int TYPES_LENGTH = 50;
    private static final AttributeType[] TYPES = new AttributeType[TYPES_LENGTH];

    static {
        for (AttributeType cop : AttributeType.values()) {
            TYPES[cop.getId()] = cop;
        }
    }

    private final int id;
    private final TypeConverters.TypeConverter converter;

    private AttributeType(int id, TypeConverters.TypeConverter converter) {
        this.id = id;
        this.converter = converter;
    }

    public int getId() {
        return id;
    }

    public TypeConverters.TypeConverter getConverter() {
        return converter;
    }

    public static AttributeType getAttributeType(int id) {
        return TYPES[id];
    }
}
