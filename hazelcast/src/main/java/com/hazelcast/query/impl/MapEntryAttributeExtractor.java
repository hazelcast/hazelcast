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
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.getters.MultiResult;
import com.hazelcast.query.impl.getters.ReflectionHelper;
import com.hazelcast.query.impl.predicates.AttributeOrigin;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.impl.TypeConverters.IDENTITY_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.NULL_CONVERTER;

public final class MapEntryAttributeExtractor {

    private MapEntryAttributeExtractor() {
    }

    public static Object extractAttributeValue(Extractors extractors, Object targetObject,
                                               Object metadata, String attributeName,
                                               AttributeOrigin attributeOrigin) {
        Object result;
        switch (attributeOrigin) {
            case KEY:
                result = targetObject;
                break;
            case VALUE:
                result = targetObject;
                break;
            case WITHIN_KEY:
                attributeName = stripKeyKeyword(attributeName);
                result = extractors.extract(targetObject, attributeName, metadata);
                break;
            case WITHIN_VALUE:
                result = extractors.extract(targetObject, attributeName, metadata);
                break;
            default:
                throw new IllegalArgumentException(attributeOrigin + " is not allowed here");
        }
        if (result instanceof HazelcastJsonValue) {
            return Json.parse(result.toString());
        }
        return result;
    }

    public static Object extractAttributeValueByOrigin(Extractors extractors, Object key, Object value,
                                                       String attributeName, AttributeOrigin origin) {
        switch (origin) {
            case KEY:
            case WITHIN_KEY:
                return extractAttributeValue(extractors, key, null, attributeName, origin);
            case VALUE:
            case WITHIN_VALUE:
                return extractAttributeValue(extractors, value, null, attributeName, origin);
            default:
                throw new IllegalArgumentException();
        }
    }

    public static Object extractAttributeValue(QueryableEntry queryableEntry, String attributeName, AttributeOrigin origin) {
        return extractAttributeValue(queryableEntry.getExtractors(), queryableEntry.getTargetObject(origin),
                getMetadataOrNull(queryableEntry, origin), attributeName, origin);
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

    /**
     * Returns a converter corresponding to the attribute with the given name.
     * Never {@code null}, but may return {@link TypeConverters#NULL_CONVERTER}
     * if the attribute value is {@code null} and therefore its type can't be
     * inferred. The latter may also happen for collection attributes if the
     * collection is empty or all its elements are {@code null}.
     */
    public static TypeConverter getConverter(QueryableEntry entry, String attributeName, AttributeOrigin attributeNameType) {
        Object attributeValue = MapEntryAttributeExtractor.extractAttributeValue(entry, attributeName, attributeNameType);
        if (attributeValue == null) {
            return NULL_CONVERTER;
        } else if (attributeValue instanceof MultiResult) {
            MultiResult multiResult = (MultiResult) attributeValue;
            for (Object result : multiResult.getResults()) {
                if (result != null) {
                    AttributeType attributeType = MapEntryAttributeExtractor.extractAttributeType(result);
                    return attributeType == null ? IDENTITY_CONVERTER : attributeType.getConverter();
                }
            }
            return NULL_CONVERTER;
        } else {
            AttributeType attributeType = MapEntryAttributeExtractor.extractAttributeType(attributeValue);
            return attributeType == null ? IDENTITY_CONVERTER : attributeType.getConverter();
        }
    }

    private static String stripKeyKeyword(String attributeName) {
        return attributeName.substring(KEY_ATTRIBUTE_NAME.value().length() + 1);
    }

    private static Object getMetadataOrNull(QueryableEntry entry, AttributeOrigin origin) {
        if (entry.getMetadata() == null) {
            return null;
        }
        switch (origin) {
            case KEY:
            case VALUE:
                return null;
            case WITHIN_KEY:
                return entry.getMetadata().getKeyMetadata();
            case WITHIN_VALUE:
                return entry.getMetadata().getValueMetadata();
            default:
                throw new IllegalArgumentException();
        }
    }
}
