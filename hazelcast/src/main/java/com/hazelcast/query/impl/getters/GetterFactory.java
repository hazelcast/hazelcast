/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.util.CollectionUtil;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;

import static com.hazelcast.query.impl.getters.AbstractMultiValueGetter.validateModifier;

public final class GetterFactory {

    private GetterFactory() {
    }

    public static Getter newFieldGetter(Object object, Getter parentGetter, Field field, String modifierSuffix)
            throws Exception {
        Class<?> fieldType = field.getType();
        Class<?> returnType = null;
        if (isExtractingFromCollection(fieldType, modifierSuffix)) {
            validateModifier(modifierSuffix);
            Object currentObject = getCurrentObject(object, parentGetter);
            if (currentObject == null) {
                return NullGetter.NULL_GETTER;
            }
            if (currentObject instanceof MultiResult) {
                MultiResult multiResult = (MultiResult) currentObject;
                returnType = extractTypeFromMultiResult(field, multiResult);
            } else {
                Collection collection = (Collection) field.get(currentObject);
                returnType = getCollectionType(collection);
            }
            if (returnType == null) {
                return NullGetter.NULL_GETTER;
            }
        } else if (isExtractingFromArray(fieldType, modifierSuffix)) {
            validateModifier(modifierSuffix);
            Object currentObject = getCurrentObject(object, parentGetter);
            if (currentObject == null) {
                return NullGetter.NULL_GETTER;
            }
        }
        return new FieldGetter(parentGetter, field, modifierSuffix, returnType);
    }

    private static Class<?> extractTypeFromMultiResult(Field field, MultiResult multiResult) throws Exception {
        Class<?> returnType = null;
        for (Object o : multiResult.getResults()) {
            if (o == null) {
                continue;
            }
            Collection collection = (Collection) field.get(o);
            returnType = getCollectionType(collection);
            if (returnType != null) {
                break;
            }
        }
        return returnType;
    }

    public static Getter newMethodGetter(Object object, Getter parentGetter, Method method, String modifierSuffix)
            throws Exception {
        Class<?> methodReturnType = method.getReturnType();
        Class<?> returnType = null;
        if (isExtractingFromCollection(methodReturnType, modifierSuffix)) {
            validateModifier(modifierSuffix);
            Object currentObject = getCurrentObject(object, parentGetter);
            if (currentObject == null) {
                return NullGetter.NULL_GETTER;
            }
            if (currentObject instanceof MultiResult) {
                MultiResult multiResult = (MultiResult) currentObject;
                returnType = extractTypeFromMultiResult(method, multiResult);
            } else {
                Collection collection = (Collection) method.invoke(currentObject);
                returnType = getCollectionType(collection);
            }
            if (returnType == null) {
                return NullGetter.NULL_GETTER;
            }
        } else if (isExtractingFromArray(methodReturnType, modifierSuffix)) {
            validateModifier(modifierSuffix);
            Object currentObject = getCurrentObject(object, parentGetter);
            if (currentObject == null) {
                return NullGetter.NULL_GETTER;
            }
        }
        return new MethodGetter(parentGetter, method, modifierSuffix, returnType);
    }

    private static Class<?> extractTypeFromMultiResult(Method method, MultiResult multiResult) throws Exception {
        Class<?> returnType = null;
        for (Object o : multiResult.getResults()) {
            if (o == null) {
                continue;
            }
            Collection collection = (Collection) method.invoke(o);
            returnType = getCollectionType(collection);
            if (returnType != null) {
                break;
            }
        }
        return returnType;
    }

    public static Getter newThisGetter(Getter parent, Object object) {
        return new ThisGetter(parent, object);
    }

    private static Class<?> getCollectionType(Collection collection) throws Exception {
        if (collection == null || collection.isEmpty()) {
            return null;
        }
        Object targetObject = CollectionUtil.getItemAtPositionOrNull(collection, 0);
        if (targetObject == null) {
            for (Object object : collection) {
                if (object != null) {
                    return object.getClass();
                }
            }
            return null;
        }
        return targetObject.getClass();
    }

    private static boolean isExtractingFromCollection(Class<?> type, String modifierSuffix) {
        return modifierSuffix != null && Collection.class.isAssignableFrom(type);
    }

    private static boolean isExtractingFromArray(Class<?> type, String modifierSuffix) {
        return modifierSuffix != null && type.isArray();
    }

    private static Object getCurrentObject(Object obj, Getter parent) throws Exception {
        return parent == null ? obj : parent.getValue(obj);
    }

}
