package com.hazelcast.query.impl.getters;

import com.hazelcast.util.ExceptionUtil;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;

public class GetterFactory {
    public static Getter newFieldGetter(Object obj, Getter parent, Field field, String reducerSuffix) {
        Class<?> type = field.getType();
        if (Collection.class.isAssignableFrom(type) && reducerSuffix != null) {
            Object currentObject = getCurrentObject(obj, parent);
            if (currentObject == null) {
                return NullGetter.NULL_GETTER;
            }

            if (currentObject instanceof MultiResultCollector) {
                MultiResultCollector multiResultCollector = (MultiResultCollector) currentObject;
                if (multiResultCollector.isEmpty()) {
                    return NullGetter.NULL_GETTER;
                }
                currentObject = multiResultCollector.getResults().iterator().next();
                if (currentObject == null) {
                    return NullGetter.NULL_GETTER;
                }
            }
            Collection targetCollection;
            try {
                targetCollection = (Collection) field.get(currentObject);
            } catch (IllegalAccessException e) {
                //TODO: What to do with the Exception?
                throw ExceptionUtil.rethrow(e);
            }
            if (targetCollection == null || targetCollection.isEmpty()) {
                return NullGetter.NULL_GETTER;
            }
            //TODO: We should take a reducer suffix into consideration
            Object targetObject = targetCollection.iterator().next();
            if (targetObject == null) {
                return NullGetter.NULL_GETTER;
            }
            return new FieldGetter(parent, field, reducerSuffix, targetObject.getClass());

        }
        return new FieldGetter(parent, field, reducerSuffix);
    }

    private static Object getCurrentObject(Object obj, Getter parent) {
        Object currentObject;
        if (parent == null) {
            currentObject = obj;
        } else {
            try {
                currentObject = parent.getValue(obj);
            } catch (Exception e) {
                //TODO: What to do with the Exception?
                throw ExceptionUtil.rethrow(e);
            }
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
