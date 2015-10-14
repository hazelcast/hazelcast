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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;

public final class GetterFactory {

    private GetterFactory() {

    }

    public static Getter newFieldGetter(Object object, Getter parentGetter, Field field, String modifierSuffix) throws Exception {
        Class<?> type = field.getType();

        Class<?> collectionType = null;
        if (extractingFromCollection(type, modifierSuffix)) {
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
        Object targetObject = extractFirstValueFromCollection(targetCollection);
        if (targetObject == null) {
            return null;
        }
        return targetObject.getClass();
    }

    private static Object extractFirstValueFromCollection(Collection targetCollection) {
        Object targetObject = targetCollection.iterator().next();
        if (targetObject == null) {
            return null;
        }
        return targetObject;
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

    private static boolean extractingFromCollection(Class<?> fieldType, String modifierSuffix) {
        return Collection.class.isAssignableFrom(fieldType) && modifierSuffix != null;
    }

    private static Object getCurrentObject(Object obj, Getter parent) throws Exception {
        Object currentObject;
        if (parent == null) {
            currentObject = obj;
        } else {
            currentObject = parent.getValue(obj);
        }
        return currentObject;
    }

}
