/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
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

    private static final FieldDefinition NULL_PORTABLE_FIELD_DEFINITION = createNullPortableFieldDefinition();

    private static final PortableFieldExtractor NULL_PORTABLE_FIELD_EXTRACTOR = createNullPortableFieldExtractor();

    private PortableExtractor() {
    }

    static Comparable extractValue(SerializationService serializationService, Data data, String fieldName)
            throws IOException {
        PortableContext portableContext = serializationService.getPortableContext();
        PortableFieldExtractor fieldExtractor = getFieldExtractor(data, fieldName, portableContext);
        PortableReader reader = serializationService.createPortableReader(data);
        return fieldExtractor.extract(reader, fieldName);
    }

    private static PortableFieldExtractor getFieldExtractor(Data data, String fieldName,
                                                            PortableContext portableContext) {

        FieldDefinition fieldDefinition = getFieldDefinition(data, fieldName, portableContext);
        if (fieldDefinition instanceof NullPortableFieldDefinition) {
            return NULL_PORTABLE_FIELD_EXTRACTOR;
        }
        int fieldType = fieldDefinition.getType().getId();
        if (fieldType < 0 || fieldType >= FIELD_EXTRACTORS.length) {
            throw new ArrayIndexOutOfBoundsException("Invalid fieldType: " + fieldType);
        }

        PortableFieldExtractor fieldExtractor = FIELD_EXTRACTORS[fieldType];
        if (fieldExtractor == null) {
            throw new QueryException("Field extractor is not defined: " + fieldType);
        }
        return fieldExtractor;
    }

    private static FieldDefinition getFieldDefinition(Data data, String fieldName, PortableContext portableContext) {
        ClassDefinition classDefinition = data.getClassDefinition();
        FieldDefinition fieldDefinition = portableContext.getFieldDefinition(classDefinition, fieldName);
        return fieldDefinition == null ? NULL_PORTABLE_FIELD_DEFINITION : fieldDefinition;
    }

    static AttributeType getAttributeType(PortableContext portableContext, Data data, String fieldName) {
        PortableFieldExtractor fieldExtractor = getFieldExtractor(data, fieldName, portableContext);
        return fieldExtractor.getAttributeType();
    }

    private static PortableFieldExtractor createNullPortableFieldExtractor() {
        return new NullPortableFieldExtractor();
    }

    private static FieldDefinition createNullPortableFieldDefinition() {
        return new NullPortableFieldDefinition();
    }

    /**
     * Represent an unknown {@link com.hazelcast.nio.serialization.Portable} field.
     */
    private static final class NullPortableFieldDefinition implements FieldDefinition {

        @Override
        public FieldType getType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getIndex() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getClassId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getFactoryId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            throw new UnsupportedOperationException();
        }
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
