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

import com.hazelcast.nio.serialization.FieldDefinition;

public enum AttributeType {
    DOUBLE(FieldDefinition.TYPE_DOUBLE, TypeConverters.DOUBLE_CONVERTER),
    LONG(FieldDefinition.TYPE_LONG, TypeConverters.LONG_CONVERTER),
    SHORT(FieldDefinition.TYPE_SHORT, TypeConverters.SHORT_CONVERTER),
    BOOLEAN(FieldDefinition.TYPE_BOOLEAN, TypeConverters.BOOLEAN_CONVERTER),
    BYTE(FieldDefinition.TYPE_BYTE, TypeConverters.BYTE_CONVERTER),
    STRING(FieldDefinition.TYPE_UTF, TypeConverters.STRING_CONVERTER),
    FLOAT(FieldDefinition.TYPE_FLOAT, TypeConverters.FLOAT_CONVERTER),
    CHAR(FieldDefinition.TYPE_CHAR, TypeConverters.CHAR_CONVERTER),
    INTEGER(FieldDefinition.TYPE_INT, TypeConverters.INTEGER_CONVERTER);

    private static final AttributeType[] types = new AttributeType[50];

    static {
        for (AttributeType cop : AttributeType.values()) {
            types[cop.getId()] = cop;
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

    public final static AttributeType getAttributeType(int id) {
        return types[id];
    }
}
