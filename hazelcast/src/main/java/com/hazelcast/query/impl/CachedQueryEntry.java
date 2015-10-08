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

import com.hazelcast.internal.serialization.PortableContext;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.getters.ReflectionHelper;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;

/**
 * Entry of the Query.
 */
public class CachedQueryEntry implements QueryableEntry {

    private Data keyData;
    private Object keyObject;
    private Data valueData;
    private Object valueObject;
    private SerializationService serializationService;

    public CachedQueryEntry() {
    }

    public CachedQueryEntry(SerializationService serializationService, Data key, Object value) {
        init(serializationService, key, value);
    }

    public void init(SerializationService serializationService, Data key, Object value) {
        if (key == null) {
            throw new IllegalArgumentException("keyData cannot be null");
        }
        this.serializationService = serializationService;
        this.keyData = key;
        this.keyObject = null;

        if (value instanceof Data) {
            this.valueData = (Data) value;
            this.valueObject = null;
        } else {
            this.valueObject = value;
            this.valueData = null;
        }
    }

    @Override
    public Object getValue() {
        if (valueObject == null) {
            valueObject = serializationService.toObject(valueData);
        }
        return valueObject;
    }

    @Override
    public Object getKey() {
        if (keyObject == null) {
            keyObject = serializationService.toObject(keyData);
        }
        return keyObject;
    }

    @Override
    public Data getKeyData() {
        if (keyData == null) {
            keyData = serializationService.toData(keyObject);
        }
        return keyData;
    }

    @Override
    public Data getValueData() {
        if (valueData == null) {
            valueData = serializationService.toData(valueObject);
        }
        return valueData;
    }


    @Override
    public Comparable getAttribute(String attributeName) throws QueryException {
        if (KEY_ATTRIBUTE_NAME.equals(attributeName)) {
            return (Comparable) getKey();
        } else if (THIS_ATTRIBUTE_NAME.equals(attributeName)) {
            return (Comparable) getValue();
        }

        boolean key = QueryEntryUtils.isKey(attributeName);
        attributeName = QueryEntryUtils.getAttributeName(key, attributeName);
        Object targetObject = getTargetObject(key);

        return QueryEntryUtils.extractAttribute(attributeName, targetObject, serializationService);
    }

    private Object getTargetObject(boolean key) {
        Object targetObject;
        if (key) {
            if (keyObject == null) {
                if (keyData.isPortable()) {
                    targetObject = keyData;
                } else {
                    targetObject = getKey();
                }
            } else {
                if (keyObject instanceof Portable) {
                    targetObject = getKeyData();
                } else {
                    targetObject = getKey();
                }
            }
        } else {
            if (valueObject == null) {
                if (valueData.isPortable()) {
                    targetObject = valueData;
                } else {
                    targetObject = getValue();
                }
            } else {
                if (valueObject instanceof Portable) {
                    targetObject = getValueData();
                } else {
                    targetObject = getValue();
                }
            }
        }
        return targetObject;
    }

    @Override
    public AttributeType getAttributeType(String attributeName) {
        if (KEY_ATTRIBUTE_NAME.equals(attributeName)) {
            return ReflectionHelper.getAttributeType(getKey().getClass());
        } else if (THIS_ATTRIBUTE_NAME.equals(attributeName)) {
            return ReflectionHelper.getAttributeType(getValue().getClass());
        }

        boolean isKey = QueryEntryUtils.isKey(attributeName);
        attributeName = QueryEntryUtils.getAttributeName(isKey, attributeName);

        Object target = getTargetObject(isKey);

        if (target instanceof Portable || target instanceof Data) {
            Data data = serializationService.toData(target);
            if (data.isPortable()) {
                PortableContext portableContext = serializationService.getPortableContext();
                return PortableExtractor.getAttributeType(portableContext, data, attributeName);
            }
        }

        return ReflectionHelper.getAttributeType(isKey ? getKey() : getValue(), attributeName);
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
        CachedQueryEntry that = (CachedQueryEntry) o;
        if (!keyData.equals(that.keyData)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return keyData.hashCode();
    }

}
