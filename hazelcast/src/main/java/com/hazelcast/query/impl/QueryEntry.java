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
    static final String KEY_ATTRIBUTE_NAME = "__key";
    static final String THIS_ATTRIBUTE_NAME = "this";
    private static PortableExtractor extractor = new PortableExtractor();
    private final SerializationService serializationService;
    Data indexKey;
    Data key;
    Object keyObject;
    Object valueObject;

    Data valueData;

    PortableReader reader = null;

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
            valueData = (Data) value;
        } else {
            valueObject = value;
        }
    }

    public Object getValue() {
        if (valueObject != null) return valueObject;
        valueObject = serializationService.toObject(valueData);
        return valueObject;
    }

    public Object getKey() {
        if (keyObject == null) {
            keyObject = serializationService.toObject(key);
        }
        return keyObject;
    }

    public Comparable getAttribute(String attributeName) throws QueryException {
        if (valueData != null && valueData.isPortable()) {
            PortableReader reader = getOrCreatePortableReader();
            return extractor.extract(reader, attributeName, valueData.getClassDefinition().get(attributeName).getType().getId());
        }
        return extractViaReflection(attributeName);
    }

    final Comparable extractViaReflection(String attributeName) {
        try {
            Object v = getValue();
            if (KEY_ATTRIBUTE_NAME.equals(attributeName)) return (Comparable) getKey();
            else if (THIS_ATTRIBUTE_NAME.equals(attributeName)) return (Comparable) v;
            return ReflectionHelper.extractValue(this, attributeName, v);
        } catch (Exception e) {
            throw new QueryException(e);
        }
    }

    public AttributeType getAttributeType(String attributeName) {
        if (valueData != null && valueData.isPortable()) {
            FieldDefinition fd = valueData.getClassDefinition().get(attributeName);
            if (fd == null) throw new QueryException("Unknown Attribute: " + attributeName);
            return AttributeType.getAttributeType(fd.getType().getId());
        }
        return getAttributeTypeViaReflection(attributeName);
    }

    private AttributeType getAttributeTypeViaReflection(String attributeName) {
        Class klass = null;
        if ("__key".equals(attributeName)) {
            klass = getKey().getClass();
        } else {
            Object value = getValue();
            if ("this".equals(attributeName)) {
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
        return key;
    }

    public Data getValueData() {
        if (valueData != null) return valueData;
        valueData = serializationService.toData(valueObject);
        return valueData;
    }

    public Data getIndexKey() {
        return indexKey;
    }

    public long getCreationTime() {
        return 0;
    }

    public long getLastAccessTime() {
        return 0;
    }

    public Object setValue(Object value) {
        throw new UnsupportedOperationException();
    }

    PortableReader getOrCreatePortableReader() {
        if (reader != null) return reader;
        return reader = serializationService.createPortableReader(valueData);
    }

    static class PortableExtractor {
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
