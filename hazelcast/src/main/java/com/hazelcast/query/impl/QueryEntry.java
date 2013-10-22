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

import com.hazelcast.nio.serialization.*;

import java.io.IOException;

public class QueryEntry implements QueryableEntry {

    private static final PortableExtractor extractor = new PortableExtractor();
    public static final String KEY_ATTRIBUTE_NAME = "__key";
    private static final String THIS_ATTRIBUTE_NAME = "this";

    private final SerializationService serializationService;
    private final Data indexKey;
    private Data key;
    private Object keyObject;
    private Data value;
    private Object valueObject;
    private PortableReader reader;

    public QueryEntry(SerializationService serializationService, Data indexKey, Object key, Object value) {
        if (indexKey == null) throw new IllegalArgumentException("index keyData cannot be null");
        if (key == null) throw new IllegalArgumentException("keyData cannot be null");
        if (value == null) throw new IllegalArgumentException("value cannot be null");
        this.indexKey = indexKey;
        if (key instanceof Data) {
            this.key = (Data) key;
        } else {
            keyObject = key;
        }
        this.serializationService = serializationService;
        if (value instanceof Data) {
            this.value = (Data) value;
        } else {
            valueObject = value;
        }
    }

    public Object getValue() {
        if (valueObject == null && serializationService != null) {
            valueObject = serializationService.toObject(value);
        }
        return valueObject;
    }

    public Object getKey() {
        if (keyObject == null && serializationService != null) {
            keyObject = serializationService.toObject(key);
        }
        return keyObject;
    }

    public Comparable getAttribute(String attributeName) throws QueryException {
        final Data data = getValueData();
        if (data != null && data.isPortable()) {
            FieldDefinition fd = data.getClassDefinition().get(attributeName);
            if (fd != null) {
                PortableReader reader = getOrCreatePortableReader();
                return extractor.extract(reader, attributeName, fd.getType().getId());
            }
        }
        return extractViaReflection(attributeName);
    }

    private Comparable extractViaReflection(String attributeName) {
        try {
            if (KEY_ATTRIBUTE_NAME.equals(attributeName)) {
                return (Comparable) getKey();
            } else if (THIS_ATTRIBUTE_NAME.equals(attributeName)) {
                return (Comparable) getValue();
            }

            if(attributeName.startsWith(KEY_ATTRIBUTE_NAME)){
                return ReflectionHelper.extractValue(this, attributeName, getKey());
            }else{
                return ReflectionHelper.extractValue(this, attributeName, getValue());
            }
        } catch (Exception e) {
            throw new QueryException(e);
        }
    }

    public AttributeType getAttributeType(String attributeName) {
        final Data data = getValueData();
        if (data != null && data.isPortable()) {
            FieldDefinition fd = data.getClassDefinition().get(attributeName);
            if (fd != null) {
                return AttributeType.getAttributeType(fd.getType().getId());
            }
        }
        return getAttributeTypeViaReflection(attributeName);
    }

    private AttributeType getAttributeTypeViaReflection(String attributeName) {
        Class klass;
        if (KEY_ATTRIBUTE_NAME.equals(attributeName)) {
            klass = getKey().getClass();
        } else {
            Object value = getValue();
            if (THIS_ATTRIBUTE_NAME.equals(attributeName)) {
                klass = value.getClass();
            } else {
                return ReflectionHelper.getAttributeType(this, attributeName);
            }
        }

        return ReflectionHelper.getAttributeType(klass);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryEntry that = (QueryEntry) o;
        if (!indexKey.equals(that.indexKey)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return indexKey.hashCode();
    }

    public Data getKeyData() {
        if (key == null && serializationService != null) {
            key = serializationService.toData(keyObject);
        }
        return key;
    }

    public Data getValueData() {
        if (value == null && serializationService != null) {
            value = serializationService.toData(valueObject);
        }
        return value;
    }

    public Data getIndexKey() {
        return indexKey;
    }

    public Object setValue(Object value) {
        throw new UnsupportedOperationException();
    }

    private PortableReader getOrCreatePortableReader() {
        if (reader != null) return reader;
        return reader = serializationService.createPortableReader(value);
    }

    private static class PortableExtractor {
        PortableFieldExtractor[] extractors = new PortableFieldExtractor[FieldType.values().length];

        PortableExtractor() {
            extractors[FieldType.BYTE.getId()] = new PortableByteFieldExtractor();
            extractors[FieldType.BOOLEAN.getId()] = new PortableBooleanFieldExtractor();
            extractors[FieldType.LONG.getId()] = new PortableLongFieldExtractor();
            extractors[FieldType.INT.getId()] = new PortableIntegerFieldExtractor();
            extractors[FieldType.CHAR.getId()] = new PortableCharFieldExtractor();
            extractors[FieldType.DOUBLE.getId()] = new PortableDoubleFieldExtractor();
            extractors[FieldType.SHORT.getId()] = new PortableShortFieldExtractor();
            extractors[FieldType.FLOAT.getId()] = new PortableFloatFieldExtractor();
            extractors[FieldType.UTF.getId()] = new PortableUtfFieldExtractor();
        }

        public Comparable extract(PortableReader reader, String fieldName, byte fieldType) throws QueryException {
            try {
                return extractors[fieldType].extract(reader, fieldName);
            } catch (IOException e) {
                throw new QueryException(e);
            }
        }

        class PortableIntegerFieldExtractor implements PortableFieldExtractor {
            public Comparable extract(PortableReader reader, String fieldName) throws IOException {
                return reader.readInt(fieldName);
            }
        }

        class PortableByteFieldExtractor implements PortableFieldExtractor {
            public Comparable extract(PortableReader reader, String fieldName) throws IOException {
                return reader.readByte(fieldName);
            }
        }

        class PortableLongFieldExtractor implements PortableFieldExtractor {
            public Comparable extract(PortableReader reader, String fieldName) throws IOException {
                return reader.readLong(fieldName);
            }
        }

        class PortableDoubleFieldExtractor implements PortableFieldExtractor {
            public Comparable extract(PortableReader reader, String fieldName) throws IOException {
                return reader.readDouble(fieldName);
            }
        }

        class PortableFloatFieldExtractor implements PortableFieldExtractor {
            public Comparable extract(PortableReader reader, String fieldName) throws IOException {
                return reader.readFloat(fieldName);
            }
        }

        class PortableShortFieldExtractor implements PortableFieldExtractor {
            public Comparable extract(PortableReader reader, String fieldName) throws IOException {
                return reader.readShort(fieldName);
            }
        }

        class PortableUtfFieldExtractor implements PortableFieldExtractor {
            public Comparable extract(PortableReader reader, String fieldName) throws IOException {
                return reader.readUTF(fieldName);
            }
        }

        class PortableCharFieldExtractor implements PortableFieldExtractor {
            public Comparable extract(PortableReader reader, String fieldName) throws IOException {
                return reader.readChar(fieldName);
            }
        }

        class PortableBooleanFieldExtractor implements PortableFieldExtractor {
            public Comparable extract(PortableReader reader, String fieldName) throws IOException {
                return reader.readBoolean(fieldName);
            }
        }

        interface PortableFieldExtractor {
            Comparable extract(PortableReader reader, String fieldName) throws IOException;
        }
    }
}
