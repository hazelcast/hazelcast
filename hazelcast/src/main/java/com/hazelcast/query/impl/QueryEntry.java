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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableContext;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.QueryException;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;
/**
 * Entry of the Query.
 */
public class QueryEntry implements QueryableEntry {

    private final SerializationService serializationService;
    private final Data indexKey;
    private Data key;
    private Object keyObject;
    private Data value;
    private Object valueObject;

    public QueryEntry(SerializationService serializationService, Data indexKey, Object key, Object value) {
        if (indexKey == null) {
            throw new IllegalArgumentException("index keyData cannot be null");
        }
        if (key == null) {
            throw new IllegalArgumentException("keyData cannot be null");
        }
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

    @Override
    public Object getValue() {
        if (valueObject == null && serializationService != null) {
            valueObject = serializationService.toObject(value);
        }
        return valueObject;
    }

    @Override
    public Object getKey() {
        if (keyObject == null && serializationService != null) {
            keyObject = serializationService.toObject(key);
        }
        return keyObject;
    }

    @Override
    public Comparable getAttribute(String attributeName) throws QueryException {
        if (KEY_ATTRIBUTE_NAME.equals(attributeName)) {
            return (Comparable) getKey();
        } else if (THIS_ATTRIBUTE_NAME.equals(attributeName)) {
            return (Comparable) getValue();
        }

        boolean key = attributeName.startsWith(KEY_ATTRIBUTE_NAME);
        Data data;
        if (key) {
            attributeName = attributeName.substring(KEY_ATTRIBUTE_NAME.length() + 1);
            data = getKeyData();
        } else {
            data = getValueData();
        }

        if (data != null && data.isPortable()) {
            return extractViaPortable(attributeName, data);
        }
        return extractViaReflection(attributeName, key);
    }

    private Comparable extractViaPortable(String attributeName, Data data) {
        try {
            return PortableExtractor.extractValue(serializationService, data, attributeName);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryException(e);
        }
    }

    private Comparable extractViaReflection(String attributeName, boolean key) {
        try {
            Object obj = key ? getKey() : getValue();
            return ReflectionHelper.extractValue(obj, attributeName);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryException(e);
        }
    }

    @Override
    public AttributeType getAttributeType(String attributeName) {
        if (KEY_ATTRIBUTE_NAME.equals(attributeName)) {
            return ReflectionHelper.getAttributeType(getKey().getClass());
        } else if (THIS_ATTRIBUTE_NAME.equals(attributeName)) {
            return ReflectionHelper.getAttributeType(getValue().getClass());
        }

        boolean key = attributeName.startsWith(KEY_ATTRIBUTE_NAME);
        Data data;
        if (key) {
            attributeName = attributeName.substring(KEY_ATTRIBUTE_NAME.length() + 1);
            data = getKeyData();
        } else {
            data = getValueData();
        }

        if (data != null && data.isPortable()) {
            PortableContext portableContext = serializationService.getPortableContext();
            return PortableExtractor.getAttributeType(portableContext, data, attributeName);
        }
        return ReflectionHelper.getAttributeType(key ? getKey() : getValue(), attributeName);
    }

    @Override
    public Data getKeyData() {
        if (key == null && serializationService != null) {
            key = serializationService.toData(keyObject);
        }
        return key;
    }

    @Override
    public Data getValueData() {
        if (value == null && serializationService != null) {
            value = serializationService.toData(valueObject);
        }
        return value;
    }

    @Override
    public Data getIndexKey() {
        return indexKey;
    }

    @Override
    public Object setValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryEntry that = (QueryEntry) o;
        if (!indexKey.equals(that.indexKey)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return indexKey.hashCode();
    }

}
