package com.hazelcast.query.impl.getters;

import com.hazelcast.query.extractor.MultiResult;
import com.hazelcast.util.ExceptionUtil;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;

public final class GetterFactory {
    public static Getter newFieldGetter(Object object, Getter parentGetter, Field field, String modifierSuffix) throws Exception {
        Class<?> type = field.getType();
        if (extractingFromCollection(type, modifierSuffix)) {
            Class<?> collectionType = getCollectionType(object, parentGetter, field);
            if (collectionType == null) {
                return NullGetter.NULL_GETTER;
            }
            return new FieldGetter(parentGetter, field, modifierSuffix, collectionType);
        }
        return new FieldGetter(parentGetter, field, modifierSuffix);
    }

    private static Class<?> getCollectionType(Object object, Getter parentGetter, Field field) throws Exception {
        Object currentObject = getCurrentObject(object, parentGetter);
        if (currentObject instanceof MultiResult) {
            currentObject = getFirstObjectFromMultiResult(currentObject);
        }
        if (currentObject == null) {
            return null;
        }

        Collection targetCollection = (Collection) field.get(object);
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

    public static Getter newMethodGetter(Getter parent, Method method) {
        return new MethodGetter(parent, method);
    }

    public static Getter newThisGetter(Getter parent, Object object) {
        return new ThisGetter(parent, object);
    }

}
