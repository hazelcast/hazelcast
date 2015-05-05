/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.PortableContext;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.QueryException;

import java.io.IOException;

/**
 * Utility class to extract a single field from a {@link com.hazelcast.nio.serialization.Portable} binary.
 */
final class PortableExtractor {

    private static final PortableFieldExtractor[] FIELD_EXTRACTORS =
            new PortableFieldExtractor[FieldType.values().length];

    static {
        FIELD_EXTRACTORS[FieldType.PORTABLE.getId()] = new PortableUnsupportedFieldExtractor();

        FIELD_EXTRACTORS[FieldType.BYTE.getId()] = new PortableByteFieldExtractor();
        FIELD_EXTRACTORS[FieldType.BOOLEAN.getId()] = new PortableBooleanFieldExtractor();
        FIELD_EXTRACTORS[FieldType.CHAR.getId()] = new PortableCharFieldExtractor();
        FIELD_EXTRACTORS[FieldType.SHORT.getId()] = new PortableShortFieldExtractor();
        FIELD_EXTRACTORS[FieldType.INT.getId()] = new PortableIntegerFieldExtractor();
        FIELD_EXTRACTORS[FieldType.LONG.getId()] = new PortableLongFieldExtractor();
        FIELD_EXTRACTORS[FieldType.FLOAT.getId()] = new PortableFloatFieldExtractor();
        FIELD_EXTRACTORS[FieldType.DOUBLE.getId()] = new PortableDoubleFieldExtractor();
        FIELD_EXTRACTORS[FieldType.UTF.getId()] = new PortableUtfFieldExtractor();

        FIELD_EXTRACTORS[FieldType.PORTABLE_ARRAY.getId()] = new PortableUnsupportedFieldExtractor();
        FIELD_EXTRACTORS[FieldType.BYTE_ARRAY.getId()] = new PortableUnsupportedFieldExtractor();
        FIELD_EXTRACTORS[FieldType.CHAR_ARRAY.getId()] = new PortableUnsupportedFieldExtractor();
        FIELD_EXTRACTORS[FieldType.SHORT_ARRAY.getId()] = new PortableUnsupportedFieldExtractor();
        FIELD_EXTRACTORS[FieldType.INT_ARRAY.getId()] = new PortableUnsupportedFieldExtractor();
        FIELD_EXTRACTORS[FieldType.LONG_ARRAY.getId()] = new PortableUnsupportedFieldExtractor();
        FIELD_EXTRACTORS[FieldType.FLOAT_ARRAY.getId()] = new PortableUnsupportedFieldExtractor();
        FIELD_EXTRACTORS[FieldType.DOUBLE_ARRAY.getId()] = new PortableUnsupportedFieldExtractor();
    }

    private static final PortableFieldExtractor NULL_PORTABLE_FIELD_EXTRACTOR = createNullPortableFieldExtractor();

    private PortableExtractor() {
    }

    static Comparable extractValue(SerializationService serializationService, Data data, String fieldName)
            throws IOException {
        PortableContext context = serializationService.getPortableContext();
        PortableFieldExtractor fieldExtractor = getFieldExtractor(context, data, fieldName);
        PortableReader reader = serializationService.createPortableReader(data);
        return fieldExtractor.extract(reader, fieldName);
    }

    private static PortableFieldExtractor getFieldExtractor(PortableContext context, Data data, String fieldName)
            throws IOException {

        ClassDefinition classDefinition = context.lookupClassDefinition(data);
        FieldDefinition fieldDefinition = context.getFieldDefinition(classDefinition, fieldName);

        if (fieldDefinition != null) {
            FieldType fieldType = fieldDefinition.getType();
            int fieldTypeId = fieldType.getId();
            if (fieldTypeId < 0 || fieldTypeId >= FIELD_EXTRACTORS.length) {
                throw new ArrayIndexOutOfBoundsException("Invalid fieldType: " + fieldTypeId);
            }

            PortableFieldExtractor fieldExtractor = FIELD_EXTRACTORS[fieldTypeId];
            if (fieldExtractor == null) {
                throw new QueryException("Field extractor is not defined: " + fieldTypeId);
            }
            return fieldExtractor;
        } else {
            return NULL_PORTABLE_FIELD_EXTRACTOR;
        }
    }

    static AttributeType getAttributeType(PortableContext portableContext, Data data, String fieldName) {
        PortableFieldExtractor fieldExtractor;
        try {
            fieldExtractor = getFieldExtractor(portableContext, data, fieldName);
        } catch (IOException e) {
            throw new QueryException(e);
        }
        return fieldExtractor.getAttributeType();
    }

    private static PortableFieldExtractor createNullPortableFieldExtractor() {
        return new NullPortableFieldExtractor();
    }

    private static class PortableIntegerFieldExtractor implements PortableFieldExtractor {
        @Override
        public Comparable extract(PortableReader reader, String fieldName) throws IOException {
            return reader.readInt(fieldName);
        }

        @Override
        public AttributeType getAttributeType() {
            return AttributeType.INTEGER;
        }
    }

    private static class PortableByteFieldExtractor implements PortableFieldExtractor {
        @Override
        public Comparable extract(PortableReader reader, String fieldName) throws IOException {
            return reader.readByte(fieldName);
        }

        @Override
        public AttributeType getAttributeType() {
            return AttributeType.BYTE;
        }
    }

    private static class PortableLongFieldExtractor implements PortableFieldExtractor {
        @Override
        public Comparable extract(PortableReader reader, String fieldName) throws IOException {
            return reader.readLong(fieldName);
        }

        @Override
        public AttributeType getAttributeType() {
            return AttributeType.LONG;
        }
    }

    private static class PortableDoubleFieldExtractor implements PortableFieldExtractor {
        @Override
        public Comparable extract(PortableReader reader, String fieldName) throws IOException {
            return reader.readDouble(fieldName);
        }

        @Override
        public AttributeType getAttributeType() {
            return AttributeType.DOUBLE;
        }
    }

    private static class PortableFloatFieldExtractor implements PortableFieldExtractor {
        @Override
        public Comparable extract(PortableReader reader, String fieldName) throws IOException {
            return reader.readFloat(fieldName);
        }

        @Override
        public AttributeType getAttributeType() {
            return AttributeType.FLOAT;
        }
    }

    private static class PortableShortFieldExtractor implements PortableFieldExtractor {
        @Override
        public Comparable extract(PortableReader reader, String fieldName) throws IOException {
            return reader.readShort(fieldName);
        }

        @Override
        public AttributeType getAttributeType() {
            return AttributeType.SHORT;
        }
    }

    private static class PortableUtfFieldExtractor implements PortableFieldExtractor {
        @Override
        public Comparable extract(PortableReader reader, String fieldName) throws IOException {
            return reader.readUTF(fieldName);
        }

        @Override
        public AttributeType getAttributeType() {
            return AttributeType.STRING;
        }
    }

    private static class PortableCharFieldExtractor implements PortableFieldExtractor {
        @Override
        public Comparable extract(PortableReader reader, String fieldName) throws IOException {
            return reader.readChar(fieldName);
        }

        @Override
        public AttributeType getAttributeType() {
            return AttributeType.CHAR;
        }
    }

    private static class PortableBooleanFieldExtractor implements PortableFieldExtractor {
        @Override
        public Comparable extract(PortableReader reader, String fieldName) throws IOException {
            return reader.readBoolean(fieldName);
        }

        @Override
        public AttributeType getAttributeType() {
            return AttributeType.BOOLEAN;
        }
    }

    private static class PortableUnsupportedFieldExtractor implements PortableFieldExtractor {
        @Override
        public Comparable extract(PortableReader reader, String fieldName) throws IOException {
            throw new UnsupportedOperationException("Unsupported Portable field in query: " + fieldName);
        }

        @Override
        public AttributeType getAttributeType() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A {@link com.hazelcast.query.impl.PortableExtractor.PortableFieldExtractor} which's methods always return null.
     * Used in cases that a portable field exists on one node but not exists on another one. In those cases returning null
     * makes a {@link com.hazelcast.query.Predicate} to return false.
     */
    private static final class NullPortableFieldExtractor implements PortableFieldExtractor {

        @Override
        public Comparable extract(PortableReader reader, String fieldName) throws IOException {
            return null;
        }

        @Override
        public AttributeType getAttributeType() {
            return null;
        }

    }

    private interface PortableFieldExtractor {

        Comparable extract(PortableReader reader, String fieldName) throws IOException;

        AttributeType getAttributeType();
    }
}
