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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
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
    private Data keyData;
    private Object keyObject;
    private Data valueData;
    private Object valueObject;

    public QueryEntry(SerializationService serializationService, Data indexKey, Object key, Object value) {
        if (indexKey == null) {
            throw new IllegalArgumentException("index keyData cannot be null");
        }
        if (key == null) {
            throw new IllegalArgumentException("keyData cannot be null");
        }

        this.indexKey = indexKey;
        this.serializationService = serializationService;

        if (key instanceof Data) {
            this.keyData = (Data) key;
        } else {
            this.keyObject = key;
        }

        if (value instanceof Data) {
            this.valueData = (Data) value;
        } else {
            this.valueObject = value;
        }
    }

    @Override
    public Object getValue() {
        // TODO: What is serialization service is null??
        if (valueObject == null && serializationService != null) {
            valueObject = serializationService.toObject(valueData);
        }
        return valueObject;
    }

    @Override
    public Object getKey() {
        // TODO: What is serialization service is null??
        if (keyObject == null && serializationService != null) {
            keyObject = serializationService.toObject(keyData);
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

        boolean isKey = isKey(attributeName);
        attributeName = getAttributeName(isKey, attributeName);
        Data targetData = getOptionalTargetData(isKey);

        // if the content is available in 'Data' format and it is portable, we can directly
        // extract the content from the targetData, without needing to deserialize
        if (targetData != null && targetData.isPortable()) {
            return extractViaPortable(attributeName, targetData);
        }

        return extractViaReflection(attributeName, isKey);
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

    // This method is very inefficient because:
    // lot of time is spend on retrieving field/method and it isn't cached
    // the actual invocation on the Field, Method is also is quite expensive.
    private Comparable extractViaReflection(String attributeName, boolean isKey) {
        try {
            Object obj = isKey ? getKey() : getValue();
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

        boolean isKey = isKey(attributeName);
        attributeName = getAttributeName(isKey, attributeName);
        Data data = getOptionalTargetData(isKey);

        if (data != null && data.isPortable()) {
            PortableContext portableContext = serializationService.getPortableContext();
            return PortableExtractor.getAttributeType(portableContext, data, attributeName);
        }
        return ReflectionHelper.getAttributeType(isKey ? getKey() : getValue(), attributeName);
    }

    private String getAttributeName(boolean isKey, String attributeName) {
        if (isKey) {
            return attributeName.substring(KEY_ATTRIBUTE_NAME.length() + 1);
        } else {
            return attributeName;
        }
    }

    /**
     * Gets the target data if available.
     *
     * If the key/value is a Portable instance, we always serialize to Data. This is inefficient, but the query
     * relies on the fields mentioned in the serialized data, not the deserialized data.
     *
     * @param isKey true if we need to key data, false for the value.
     * @return the target Data. Could be null
     */
    private Data getOptionalTargetData(boolean isKey) {
        if (isKey) {
            if (keyObject instanceof Portable) {
                return getKeyData();
            } else {
                return keyData;
            }
        } else {
            if (valueObject instanceof Portable) {
                return getValueData();
            } else {
                return valueData;
            }
        }
    }

    public boolean isKey(String attributeName) {
        return attributeName.startsWith(KEY_ATTRIBUTE_NAME);
    }

    @Override
    public Data getKeyData() {
        if (keyData == null && serializationService != null) {
            keyData = serializationService.toData(keyObject);
        }
        return keyData;
    }

    @Override
    public Data getValueData() {
        if (valueData == null && serializationService != null) {
            valueData = serializationService.toData(valueObject);
        }
        return valueData;
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
