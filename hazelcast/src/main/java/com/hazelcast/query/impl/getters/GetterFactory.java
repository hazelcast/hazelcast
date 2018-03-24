/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.query.impl.getters.AbstractMultiValueGetter.validateMapKey;
import static com.hazelcast.query.impl.getters.AbstractMultiValueGetter.validateModifier;
import static com.hazelcast.query.impl.getters.NullGetter.NULL_GETTER;
import static com.hazelcast.query.impl.getters.NullMultiValueGetter.NULL_MULTIVALUE_GETTER;

import com.hazelcast.util.CollectionUtil;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

public final class GetterFactory {

    private static final String ANY_POSTFIX = "[any]";
    private static final AccessType<Field> FIELD = new AccessType<Field>() {
        @Override
        public Object getObject(Field field, Object fromObject) throws IllegalAccessException {
            return field.get(fromObject);
        }

        @Override
        public Class<?> getType(Field field) {
            return field.getType();
        }

        @Override
        public Getter createGetter(Getter parentGetter, Field field, String modifierSuffix, Class<?> returnType) {
            return new FieldGetter(parentGetter, field, modifierSuffix, returnType);
        }
    };

    private static final AccessType<Method> METHOD = new AccessType<Method>() {
        @Override
        public Object getObject(Method method, Object fromObject)
                throws IllegalAccessException, InvocationTargetException {
            return method.invoke(fromObject);
        }

        @Override
        public Class<?> getType(Method method) {
            return method.getReturnType();
        }

        @Override
        public Getter createGetter(Getter parentGetter, Method method, String modifierSuffix, Class<?> returnType) {
            return new MethodGetter(parentGetter, method, modifierSuffix, returnType);
        }
    };

    private GetterFactory() {
    }

    public static Getter newFieldGetter(Object object, Getter parentGetter, Field field, String modifierSuffix)
            throws Exception {
        return createGetterForAccessibleObject(FIELD, field, object, parentGetter, modifierSuffix);
    }

    private static Getter createGetterForAccessibleObject(AccessType accessType, AccessibleObject accessibleObject,
            Object object, Getter parentGetter,
            String modifierSuffix) throws Exception {
        Class<?> objectType = accessType.getType(accessibleObject);
        Class<?> returnType = null;
        if (isExtractingFromCollection(objectType, modifierSuffix)) {
            validateModifier(modifierSuffix);
            Object currentObject = getCurrentObject(object, parentGetter);
            if (currentObject == null) {
                return NULL_GETTER;
            }
            if (currentObject instanceof MultiResult) {
                MultiResult multiResult = (MultiResult) currentObject;
                returnType = extractTypeFromMultiResult(accessibleObject, multiResult, accessType);
            } else {
                Collection collection = (Collection) accessType.getObject(accessibleObject, currentObject);
                returnType = getCollectionType(collection);
            }
            if (returnType == null) {
                if (modifierSuffix.equals(ANY_POSTFIX)) {
                    return NULL_MULTIVALUE_GETTER;
                }
                return NULL_GETTER;
            }
        } else if (isExtractingFromArray(objectType, modifierSuffix)) {
            validateModifier(modifierSuffix);
            Object currentObject = getCurrentObject(object, parentGetter);
            if (currentObject == null) {
                return NULL_GETTER;
            }
        } else if (isExtractingFromMap(objectType, modifierSuffix)) {
            returnType = defineReturnTypeForMap(object, parentGetter, accessibleObject, modifierSuffix, accessType);
            if (returnType == null) {
                return getNullGetterOrMultivalueNullGetter(modifierSuffix);
            }
        }
        return accessType.createGetter(parentGetter, accessibleObject, modifierSuffix, returnType);
    }

    private static Class<?> extractTypeFromMultiResult(AccessibleObject accessibleObject, MultiResult multiResult,
            AccessType accessType) throws Exception {
        Class<?> returnType = null;
        for (Object o : multiResult.getResults()) {
            if (o == null) {
                continue;
            }
            Collection collection = (Collection) accessType.getObject(accessibleObject, o);
            returnType = getCollectionType(collection);
            if (returnType != null) {
                break;
            }
        }
        return returnType;
    }

    public static Getter newMethodGetter(Object object, Getter parentGetter, Method method, String modifierSuffix)
            throws Exception {
        return createGetterForAccessibleObject(METHOD, method, object, parentGetter, modifierSuffix);
    }

    private static Getter getNullGetterOrMultivalueNullGetter(String modifierSuffix) {
        if (modifierSuffix.equals(ANY_POSTFIX)) {
            return NULL_MULTIVALUE_GETTER;
        }
        return NULL_GETTER;
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

    private static Class<?> defineReturnTypeForMap(Object object, Getter parentGetter,
            AccessibleObject accessibleObject, String modifierSuffix, AccessType accessType) throws Exception {
        validateMapKey(modifierSuffix);
        Object currentObject = getCurrentObject(object, parentGetter);
        if (currentObject == null) {
            return null;
        }
        Map<?, ?> map = (Map<?, ?>) accessType.getObject(accessibleObject, currentObject);
        if (map.isEmpty()) {
            return null;
        }
        return getMapValueType(map);
    }

    private static Class<?> getMapValueType(Map<?, ?> map) {
        for (Entry<?, ?> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (value != null) {
                return value.getClass();
            }
        }
        return null;
    }

    private static boolean isExtractingFromCollection(Class<?> type, String modifierSuffix) {
        return modifierSuffix != null && Collection.class.isAssignableFrom(type);
    }

    private static boolean isExtractingFromArray(Class<?> type, String modifierSuffix) {
        return modifierSuffix != null && type.isArray();
    }

    private static boolean isExtractingFromMap(Class<?> type, String modifierSuffix) {
        return modifierSuffix != null && Map.class.isAssignableFrom(type);
    }

    private static Object getCurrentObject(Object obj, Getter parent) throws Exception {
        return parent == null ? obj : parent.getValue(obj);
    }

    private interface AccessType<T extends AccessibleObject> {

        Object getObject(T accessibleObject, Object fromObject)
                throws IllegalAccessException, InvocationTargetException;

        Class<?> getType(T field);

        Getter createGetter(Getter parentGetter, T accessObject, String modifierSuffix, Class<?> returnType);
    }
}
