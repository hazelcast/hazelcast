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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.Metadata;
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
 * <p>
 * If the object, which is used as the extraction target, is not of Data or Portable type the serializationService
 * will not be touched at all.
 */
public abstract class QueryableEntry<K, V> implements Extractable, Map.Entry<K, V> {

    protected InternalSerializationService serializationService;
    protected Extractors extractors;

    private Metadata metadata;

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public Object getAttributeValue(String attributeName) throws QueryException {
        return extractAttributeValue(attributeName);
    }

    public abstract V getValue();

    public abstract K getKey();

    public abstract Data getKeyData();

    public abstract Data getValueData();

    protected abstract Object getTargetObject(boolean key);

    /**
     * Returns a converter corresponding to the attribute with the given name.
     * Never {@code null}, but may return {@link TypeConverters#NULL_CONVERTER}
     * if the attribute value is {@code null} and therefore its type can't be
     * inferred. The latter may also happen for collection attributes if the
     * collection is empty or all its elements are {@code null}.
     */
    TypeConverter getConverter(String attributeName) {
        Object attributeValue = getAttributeValue(attributeName);
        if (attributeValue == null) {
            return NULL_CONVERTER;
        } else if (attributeValue instanceof MultiResult) {
            MultiResult multiResult = (MultiResult) attributeValue;
            for (Object result : multiResult.getResults()) {
                if (result != null) {
                    AttributeType attributeType = extractAttributeType(result);
                    return attributeType == null ? IDENTITY_CONVERTER : attributeType.getConverter();
                }
            }
            return NULL_CONVERTER;
        } else {
            AttributeType attributeType = extractAttributeType(attributeValue);
            return attributeType == null ? IDENTITY_CONVERTER : attributeType.getConverter();
        }
    }

    private Object extractAttributeValue(String attributeName) throws QueryException {
        Object result = extractAttributeValueIfAttributeQueryConstant(attributeName);
        if (result == null) {
            boolean isKey = startsWithKeyConstant(attributeName);
            attributeName = getAttributeName(isKey, attributeName);
            Object target = getTargetObject(isKey);
            Object metadata = getMetadataOrNull(this.metadata, isKey);
            result = extractAttributeValueFromTargetObject(extractors, attributeName, target, metadata);
        }
        if (result instanceof HazelcastJsonValue) {
            return Json.parse(result.toString());
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
                                        String attributeName, Data key, Object value, Object metadata) throws QueryException {
        Object result = extractAttributeValueIfAttributeQueryConstant(serializationService, attributeName, key, value);
        if (result == null) {
            boolean isKey = startsWithKeyConstant(attributeName);
            attributeName = getAttributeName(isKey, attributeName);
            Object target = isKey ? key : value;
            result = extractAttributeValueFromTargetObject(extractors, attributeName, target, metadata);
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
                                                                String attributeName, Object target, Object metadata) {
        return extractors.extract(target, attributeName, metadata);
    }

    /**
     * Deduces the {@link AttributeType} of the given non-{@code null} attribute
     * value.
     *
     * @param attributeValue the attribute value to deduce the type of.
     * @return the deduced attribute type or {@code null} if there is no
     * attribute type corresponding to the type of the value. See {@link
     * AttributeType} for the list of representable attribute types.
     */
    public static AttributeType extractAttributeType(Object attributeValue) {
        if (attributeValue instanceof Portable) {
            return AttributeType.PORTABLE;
        }
        return ReflectionHelper.getAttributeType(attributeValue.getClass());
    }

    private static Object getMetadataOrNull(Metadata metadata, boolean isKey) {
        if (metadata == null) {
            return null;
        }
        return isKey ? metadata.getKeyMetadata() : metadata.getValueMetadata();
    }

}
