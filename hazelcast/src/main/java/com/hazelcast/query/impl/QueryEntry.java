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

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;

import com.hazelcast.internal.serialization.PortableContext;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.getters.ReflectionHelper;

/**
 * Entry of the Query.
 */
public class QueryEntry implements QueryableEntry {

    private Data indexKey;
    private Object key;
    private Object value;
    private SerializationService serializationService;

    public QueryEntry() {
    }

    public QueryEntry(SerializationService serializationService, Data indexKey, Object key, Object value) {
        init(serializationService, indexKey, key, value);
    }

    /**
     * It may be useful to use this {@code init} method in some cases that same instance of this class can be used
     * instead of creating a new one for every iteration when scanning large data sets, for example:
     * <pre>
     * <code>Predicate predicate = ...
     * QueryEntry entry = new QueryEntry()
     * for(i == 0; i < HUGE_NUMBER; i++) {
     *       entry.init(...)
     *       boolean valid = predicate.apply(queryEntry);
     *
     *       if(valid) {
     *          ....
     *       }
     *  }
     * </code>
     * </pre>
     */
    public void init(SerializationService serializationService, Data indexKey, Object key, Object value) {
        if (indexKey == null) {
            throw new IllegalArgumentException("index keyData cannot be null");
        }
        if (key == null) {
            throw new IllegalArgumentException("keyData cannot be null");
        }

        this.indexKey = indexKey;
        this.serializationService = serializationService;

        this.key = key;
        this.value = value;
    }

    @Override
    public Object getValue() {
        return serializationService.toObject(value);
    }

    @Override
    public Object getKey() {
        return serializationService.toObject(key);
    }

    @Override
    public Data getKeyData() {
        return indexKey;
    }

    @Override
    public Data getValueData() {
        return serializationService.toData(value);
    }


    @Override
    public Comparable getAttribute(String attributeName) throws QueryException {
        return extractAttribute(attributeName, key, value, serializationService);
    }

    public static Comparable extractAttribute(String attributeName, Object key, Object value, SerializationService ss) {
        if (KEY_ATTRIBUTE_NAME.equals(attributeName)) {
            return (Comparable) ss.toObject(key);
        } else if (THIS_ATTRIBUTE_NAME.equals(attributeName)) {
            return (Comparable) ss.toObject(value);
        }

        boolean isKey = isKey(attributeName);
        attributeName = getAttributeName(isKey, attributeName);
        Object target = isKey ? key : value;

        if (target instanceof Portable || target instanceof Data) {
            Data targetData = ss.toData(target);
            if (targetData.isPortable()) {
                return extractViaPortable(attributeName, targetData, ss);
            }
        }

        Object targetObject = ss.toObject(target);

        return extractViaReflection(attributeName, targetObject);
    }

    static Comparable extractViaPortable(String attributeName, Data data, SerializationService ss) {
        try {
            return PortableExtractor.extractValue(ss, data, attributeName);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryException(e);
        }
    }

    // This method is very inefficient because:
    // lot of time is spend on retrieving field/method and it isn't cached
    // the actual invocation on the Field, Method is also is quite expensive.
    public static Comparable extractViaReflection(String attributeName, Object obj) {
        try {
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

        Object target = isKey ? key : value;

        if (target instanceof Portable || target instanceof Data) {
            Data data = serializationService.toData(target);
            if (data.isPortable()) {
                PortableContext portableContext = serializationService.getPortableContext();
                return PortableExtractor.getAttributeType(portableContext, data, attributeName);
            }
        }

        return ReflectionHelper.getAttributeType(isKey ? getKey() : getValue(), attributeName);
    }

    private static String getAttributeName(boolean isKey, String attributeName) {
        if (isKey) {
            return attributeName.substring(KEY_ATTRIBUTE_NAME.length() + 1);
        } else {
            return attributeName;
        }
    }

    public static boolean isKey(String attributeName) {
        return attributeName.startsWith(KEY_ATTRIBUTE_NAME);
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
