/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.record.Record;
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
 * <p>
 * If the object, which is used as the extraction target, is not of Data or Portable type the serializationService
 * will not be touched at all.
 */
public abstract class QueryableEntry<K, V> implements Extractable, Map.Entry<K, V> {

    protected InternalSerializationService serializationService;
    protected Extractors extractors;

    protected Record record;
    private transient JsonMetadata metadata;

    public Record getRecord() {
        return record;
    }

    public void setRecord(Record record) {
        this.record = record;
    }

    @Override
    public Object getAttributeValue(String attributeName) throws QueryException {
        return extractAttributeValue(attributeName);
    }

    public abstract K getKey();

    public abstract Data getKeyData();

    public abstract V getValue();

    public abstract Data getValueData();

    public abstract K getKeyIfPresent();

    public abstract Data getKeyDataIfPresent();

    public abstract V getValueIfPresent();

    public abstract Data getValueDataIfPresent();

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
            Object metadata = getMetadataOrNull(isKey);
            result = extractors.extract(target, attributeName, metadata);
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

    private static boolean startsWithKeyConstant(String attributeName) {
        return attributeName.startsWith(KEY_ATTRIBUTE_NAME.value() + ".");
    }

    private static String getAttributeName(boolean isKey, String attributeName) {
        if (isKey) {
            return attributeName.substring(KEY_ATTRIBUTE_NAME.value().length() + 1);
        } else {
            return attributeName;
        }
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

    private Object getMetadataOrNull(boolean isKey) {
        if (metadata == null) {
            return null;
        }
        return isKey ? metadata.getKeyMetadata() : metadata.getValueMetadata();
    }

    public JsonMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(JsonMetadata metadata) {
        this.metadata = metadata;
    }

}
