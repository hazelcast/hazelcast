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

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.extractor.MultiResult;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.query.impl.getters.ReflectionHelper;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;

final class ExtractionEngine {

    private ExtractionEngine() {
    }

    // Data key
    public static Object extractAttributeValue(Extractors extractors, SerializationService ss, String attributeName,
                                               Data key, Object value) {
        if (KEY_ATTRIBUTE_NAME.value().equals(attributeName)) {
            return ss.toObject(key);
        } else if (THIS_ATTRIBUTE_NAME.value().equals(attributeName)) {
            return ss.toObject(value);
        }

        boolean isKey = isKey(attributeName);
        attributeName = getAttributeName(isKey, attributeName);
        Object target = isKey ? key : value;

        return doExtractAttributeValue(extractors, ss, attributeName, target);
    }

    public static Object extractAttributeValue(Extractors extractors, SerializationService ss, String attributeName,
                                               QueryableEntry entry) throws QueryException {
        if (KEY_ATTRIBUTE_NAME.value().equals(attributeName)) {
            return entry.getKey();
        } else if (THIS_ATTRIBUTE_NAME.value().equals(attributeName)) {
            return entry.getValue();
        }

        boolean isKey = isKey(attributeName);
        attributeName = getAttributeName(isKey, attributeName);
        Object target = entry.getTargetObject(isKey);

        return doExtractAttributeValue(extractors, ss, attributeName, target);
    }

    static boolean isKey(String attributeName) {
        return attributeName.startsWith(KEY_ATTRIBUTE_NAME.value());
    }

    static String getAttributeName(boolean isKey, String attributeName) {
        if (isKey) {
            return attributeName.substring(KEY_ATTRIBUTE_NAME.value().length() + 1);
        } else {
            return attributeName;
        }
    }

    static Object doExtractAttributeValue(Extractors extractors, SerializationService ss,
                                          String attributeName, Object target) {
        if (target instanceof Portable || target instanceof Data) {
            Data targetData = ss.toData(target);
            if (targetData.isPortable()) {
                return extractViaPortable(attributeName, targetData, ss);
            }
        }

        Object targetObject = ss.toObject(target);
        ValueExtractor extractor = extractors.getExtractor(attributeName);
        if (extractor != null) {
            return extractor.extract(targetObject);
        } else {
            return extractViaReflection(attributeName, targetObject);
        }
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
    static Object extractViaReflection(String attributeName, Object obj) {
        try {
            return ReflectionHelper.extractValue(obj, attributeName);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryException(e);
        }
    }

    public static AttributeType extractAttributeType(Extractors extractors, SerializationService ss,
                                                     String attributeName, QueryableEntry entry, Object attribute) {
        //TODO: This signature looks a bit suspicious. The attribute may or may not be null.
        if (KEY_ATTRIBUTE_NAME.value().equals(attributeName)) {
            return ReflectionHelper.getAttributeType(entry.getKey().getClass());
        } else if (THIS_ATTRIBUTE_NAME.value().equals(attributeName)) {
            return ReflectionHelper.getAttributeType(entry.getValue().getClass());
        }

        if (attribute == null) {
            attribute = extractAttributeValue(extractors, ss, attributeName, entry);
        }
        return getExtractedAttributeType(attribute);
    }

    static AttributeType getExtractedAttributeType(Object extractedObject) {
        if (extractedObject instanceof MultiResult) {
            return getExtractedAttributeTypeFromMultiResult((MultiResult) extractedObject);
        } else {
            return getExtractedAttributeTypeFromSingleResult(extractedObject);
        }
    }

    static AttributeType getExtractedAttributeTypeFromSingleResult(Object extractedObject) {
        if (extractedObject == null) {
            return null;
        } else {
            return ReflectionHelper.getAttributeType(extractedObject.getClass());
        }
    }

    static AttributeType getExtractedAttributeTypeFromMultiResult(MultiResult extractedMultiResult) {
        if (extractedMultiResult.isEmpty()) {
            return null;
        } else {
            return ReflectionHelper.getAttributeType(extractedMultiResult.getResults().get(0).getClass());
        }
    }


}
