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
import com.hazelcast.query.impl.getters.ReflectionHelper;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;

final class QueryEntryUtils {

    private QueryEntryUtils() {

    }

    public static boolean isKey(String attributeName) {
        return attributeName.startsWith(KEY_ATTRIBUTE_NAME);
    }

    static String getAttributeName(boolean isKey, String attributeName) {
        if (isKey) {
            return attributeName.substring(KEY_ATTRIBUTE_NAME.length() + 1);
        } else {
            return attributeName;
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


    public static Comparable extractAttribute(String attributeName, Object key, Object value, SerializationService ss) {
        if (KEY_ATTRIBUTE_NAME.equals(attributeName)) {
            return (Comparable) ss.toObject(key);
        } else if (THIS_ATTRIBUTE_NAME.equals(attributeName)) {
            return (Comparable) ss.toObject(value);
        }

        boolean isKey = isKey(attributeName);
        attributeName = getAttributeName(isKey, attributeName);
        Object target = isKey ? key : value;

        return extractAttribute(attributeName, target, ss);
    }

    public static Comparable extractAttribute(String attributeName, Object target, SerializationService ss) {
        if (target instanceof Portable || target instanceof Data) {
            Data targetData = ss.toData(target);
            if (targetData.isPortable()) {
                return extractViaPortable(attributeName, targetData, ss);
            }
        }

        Object targetObject = ss.toObject(target);
        return QueryEntryUtils.extractViaReflection(attributeName, targetObject);
    }

    // This method is very inefficient because:
    // lot of time is spend on retrieving field/method and it isn't cached
    // the actual invocation on the Field, Method is also is quite expensive.
    static Comparable extractViaReflection(String attributeName, Object obj) {
        try {
            return ReflectionHelper.extractValue(obj, attributeName);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryException(e);
        }
    }
}
