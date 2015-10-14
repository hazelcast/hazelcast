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

package com.hazelcast.query.impl.getters;

import com.hazelcast.query.extractor.MultiResult;
import com.hazelcast.util.CollectionUtil;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;

public final class GetterFactory {

    private GetterFactory() {

    }

    public static Getter newFieldGetter(Object object, Getter parentGetter, Field field, String modifierSuffix) throws Exception {
        Class<?> type = field.getType();

        Class<?> collectionType = null;
        if (isExtractingFromCollection(type, modifierSuffix)) {
            Object currentObject = getCurrentObject(object, parentGetter);
            collectionType = getCollectionType(currentObject, field);
            if (collectionType == null) {
                return NullGetter.NULL_GETTER;
            }
        }
        return new FieldGetter(parentGetter, field, modifierSuffix, collectionType);
    }

    public static Getter newMethodGetter(Getter parent, Method method) {
        return new MethodGetter(parent, method);
    }

    public static Getter newThisGetter(Getter parent, Object object) {
        return new ThisGetter(parent, object);
    }

    private static Class<?> getCollectionType(Object currentObject, Field field) throws Exception {
        if (currentObject instanceof MultiResult) {
            currentObject = getFirstObjectFromMultiResult(currentObject);
        }
        if (currentObject == null) {
            return null;
        }

        Collection targetCollection = (Collection) field.get(currentObject);
        if (targetCollection == null || targetCollection.isEmpty()) {
            return null;
        }
        Object targetObject = CollectionUtil.getItemAtPositionOrNull(targetCollection, 0);
        if (targetObject == null) {
            return null;
        }
        return targetObject.getClass();
    }

    private static Object getFirstObjectFromMultiResult(Object currentObject) {
        MultiResult multiResult = (MultiResult) currentObject;
        if (multiResult.isEmpty()) {
            return null;
        }
        currentObject = multiResult.getResults().iterator().next();
        if (currentObject == null) {
            return null;
        }
        return currentObject;
    }

    private static boolean isExtractingFromCollection(Class<?> fieldType, String modifierSuffix) {
        return modifierSuffix != null && Collection.class.isAssignableFrom(fieldType);
    }

    private static Object getCurrentObject(Object obj, Getter parent) throws Exception {
        return parent == null ? obj : parent.getValue(obj);
    }

}
