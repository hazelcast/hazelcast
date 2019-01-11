/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.getters.MultiResult;
import com.hazelcast.query.impl.getters.ReflectionHelper;

import java.util.Map;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;
import static com.hazelcast.query.impl.TypeConverters.IDENTITY_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.NULL_CONVERTER;

/**
 * This abstract class contains methods related to Queryable Entry, which means searched an indexed by SQL query or predicate.
 * <p/>
 * If the object, which is used as the extraction target, is not of Data or Portable type the serializationService
 * will not be touched at all.
 */
public abstract class QueryableEntry<K, V> implements Extractable, Map.Entry<K, V> {

    protected InternalSerializationService serializationService;
    protected Extractors extractors;

    @Override
    public Object getAttributeValue(String attributeName) throws QueryException {
        return extractAttributeValue(attributeName);
    }

    public abstract V getValue();

    public abstract K getKey();

    public abstract Data getKeyData();

    public abstract Data getValueData();

    protected abstract Object getTargetObject(boolean key);

    TypeConverter getConverter(String attributeName) {
        Object attribute = getAttributeValue(attributeName);
        if (attribute == null) {
            return NULL_CONVERTER;
        } else {
            AttributeType attributeType = extractAttributeType(attribute);
            return attributeType == null ? IDENTITY_CONVERTER : attributeType.getConverter();
        }
    }

    private Object extractAttributeValue(String attributeName) throws QueryException {
        Object result = extractAttributeValueIfAttributeQueryConstant(attributeName);
        if (result == null) {
            boolean isKey = startsWithKeyConstant(attributeName);
            attributeName = getAttributeName(isKey, attributeName);
            Object target = getTargetObject(isKey);
            result = extractAttributeValueFromTargetObject(extractors, attributeName, target);
        }
        return result;
    }

    /**
     * Optimized version of the other extractAttributeValueIfAttributeQueryConstant() method that uses getKey() and
     * getValue() calls that may cache their results internally - like in CachedQueryEntry.
     */
    private Object extractAttributeValueIfAttributeQueryConstant(String attributeName) {
        if (KEY_ATTRIBUTE_NAME.value().equals(attributeName)) {
            return getKey();
        } else if (THIS_ATTRIBUTE_NAME.value().equals(attributeName)) {
            return getValue();
        }
        return null;
    }

    /**
     * Static version of the extractAttributeValue() method used when the caller does not have
     * an instance of the QueryableEntry, but is in possession of key and value.
     */
    static Object extractAttributeValue(Extractors extractors, InternalSerializationService serializationService,
                                        String attributeName, Data key, Object value) throws QueryException {
        Object result = extractAttributeValueIfAttributeQueryConstant(serializationService, attributeName, key, value);
        if (result == null) {
            boolean isKey = startsWithKeyConstant(attributeName);
            attributeName = getAttributeName(isKey, attributeName);
            Object target = isKey ? key : value;
            result = extractAttributeValueFromTargetObject(extractors, attributeName, target);
        }
        return result;
    }

    /**
     * Static version of the extractAttributeValueIfAttributeQueryConstant() method that needs key and value upfront.
     */
    private static Object extractAttributeValueIfAttributeQueryConstant(InternalSerializationService serializationService,
                                                                        String attributeName, Data key, Object value) {
        if (KEY_ATTRIBUTE_NAME.value().equals(attributeName)) {
            return serializationService.toObject(key);
        } else if (THIS_ATTRIBUTE_NAME.value().equals(attributeName)) {
            return value instanceof Data ? serializationService.toObject(value) : value;
        }
        return null;
    }

    private static boolean startsWithKeyConstant(String attributeName) {
        return attributeName.startsWith(KEY_ATTRIBUTE_NAME.value());
    }

    private static String getAttributeName(boolean isKey, String attributeName) {
        if (isKey) {
            return attributeName.substring(KEY_ATTRIBUTE_NAME.value().length() + 1);
        } else {
            return attributeName;
        }
    }

    private static Object extractAttributeValueFromTargetObject(Extractors extractors,
                                                                String attributeName, Object target) {
        return extractors.extract(target, attributeName);
    }

    public static AttributeType extractAttributeType(Object attributeValue) {
        if (attributeValue instanceof MultiResult) {
            return extractAttributeTypeFromMultiResult((MultiResult) attributeValue);
        } else {
            return extractAttributeTypeFromSingleResult(attributeValue);
        }
    }

    private static AttributeType extractAttributeTypeFromJsonValue(JsonValue value) {
        if (value.isNumber()) {
            // toString method does not do any encoding in number case, it just returns stored string.
            if (value.toString().contains(".")) {
                // floating point number
                return AttributeType.DOUBLE;
            } else {
                return AttributeType.LONG;
            }
        } else if (value.isBoolean()) {
            return AttributeType.BOOLEAN;
        } else if (value.isString()) {
            return AttributeType.STRING;
        }
        throw new HazelcastSerializationException("Unknown Json type: " + value);
    }

    private static AttributeType extractAttributeTypeFromSingleResult(Object extractedSingleResult) {
        if (extractedSingleResult == null) {
            return null;
        }
        if (extractedSingleResult instanceof HazelcastJsonValue) {
            return extractAttributeTypeFromJsonValue(Json.parse(extractedSingleResult.toString()));
        }
        if (extractedSingleResult instanceof Portable) {
            return AttributeType.PORTABLE;
        }
        return ReflectionHelper.getAttributeType(extractedSingleResult.getClass());

    }

    private static AttributeType extractAttributeTypeFromMultiResult(MultiResult extractedMultiResult) {
        Object firstNonNullResult = null;
        for (Object result : extractedMultiResult.getResults()) {
            if (result != null) {
                firstNonNullResult = result;
                break;
            }
        }
        if (firstNonNullResult == null) {
            return null;
        }
        return ReflectionHelper.getAttributeType(firstNonNullResult.getClass());
    }

}
