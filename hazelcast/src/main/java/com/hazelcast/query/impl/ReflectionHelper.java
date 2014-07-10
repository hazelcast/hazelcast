/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.query.QueryException;
import com.hazelcast.util.EmptyStatement;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;

/**
 * Scans your classpath, indexes the metadata, allows you to query it on runtime.
 */
public final class ReflectionHelper {

    private static final ClassLoader THIS_CL = ReflectionHelper.class.getClassLoader();
    private static final ConcurrentMap<String, Getter> GETTER_CACHE = new ConcurrentHashMap<String, Getter>(1000);
    private static final int INITIAL_CAPACITY = 3;

    private ReflectionHelper() {
    }

    public static AttributeType getAttributeType(Class klass) {
        if (klass == String.class) {
            return AttributeType.STRING;
        } else if (klass == int.class || klass == Integer.class) {
            return AttributeType.INTEGER;
        } else if (klass == long.class || klass == Long.class) {
            return AttributeType.LONG;
        } else if (klass == boolean.class || klass == Boolean.class) {
            return AttributeType.BOOLEAN;
        } else if (klass == double.class || klass == Double.class) {
            return AttributeType.DOUBLE;
        } else if (klass == BigDecimal.class) {
            return AttributeType.BIG_DECIMAL;
        } else if (klass == BigInteger.class) {
            return AttributeType.BIG_INTEGER;
        } else if (klass == float.class || klass == Float.class) {
            return AttributeType.FLOAT;
        } else if (klass == byte.class || klass == Byte.class) {
            return AttributeType.BYTE;
        } else if (klass == char.class || klass == Character.class) {
            return AttributeType.CHAR;
        } else if (klass == Timestamp.class) {
            return AttributeType.SQL_TIMESTAMP;
        } else if (klass == java.sql.Date.class) {
            return AttributeType.SQL_DATE;
        } else if (klass == Date.class) {
            return AttributeType.DATE;
        } else if (klass.isEnum()) {
            return AttributeType.ENUM;
        }
        return null;
    }

    public static void reset() {
        GETTER_CACHE.clear();
    }

    public static AttributeType getAttributeType(QueryableEntry entry, String attribute) {
        return getAttributeType(createGetter(entry, attribute).getReturnType());
    }

    private static Getter createGetter(QueryableEntry entry, String attribute) {
        String paramAttribute = attribute;
        Object obj;
        if (paramAttribute.startsWith(KEY_ATTRIBUTE_NAME)) {
            obj = entry.getKey();
            if (paramAttribute.length() > KEY_ATTRIBUTE_NAME.length()) {
                paramAttribute = paramAttribute.substring(KEY_ATTRIBUTE_NAME.length() + 1);
            }
        } else {
            obj = entry.getValue();
        }

        Class clazz = obj.getClass();
        final String cacheKey = clazz.getName() + ":" + paramAttribute;
        Getter getter = GETTER_CACHE.get(cacheKey);
        if (getter != null) {
            return getter;
        }

        try {
            Getter parent = null;
            List<String> possibleMethodNames = new ArrayList<String>(INITIAL_CAPACITY);
            for (final String name : paramAttribute.split("\\.")) {
                Getter localGetter = null;
                possibleMethodNames.clear();
                possibleMethodNames.add(name);
                final String camelName = Character.toUpperCase(name.charAt(0)) + name.substring(1);
                possibleMethodNames.add("get" + camelName);
                possibleMethodNames.add("is" + camelName);
                if (name.equals(THIS_ATTRIBUTE_NAME)) {
                    localGetter = new ThisGetter(parent, obj);
                } else {
                    for (String methodName : possibleMethodNames) {
                        try {
                            final Method method = clazz.getMethod(methodName);
                            method.setAccessible(true);
                            localGetter = new MethodGetter(parent, method);
                            clazz = method.getReturnType();
                            break;
                        } catch (NoSuchMethodException ignored) {
                            EmptyStatement.ignore(ignored);
                        }
                    }
                    if (localGetter == null) {
                        try {
                            final Field field = clazz.getField(name);
                            localGetter = new FieldGetter(parent, field);
                            clazz = field.getType();
                        } catch (NoSuchFieldException ignored) {
                            EmptyStatement.ignore(ignored);
                        }
                    }
                    if (localGetter == null) {
                        Class c = clazz;
                        while (!Object.class.equals(c)) {
                            try {
                                final Field field = c.getDeclaredField(name);
                                field.setAccessible(true);
                                localGetter = new FieldGetter(parent, field);
                                clazz = field.getType();
                                break;
                            } catch (NoSuchFieldException ignored) {
                                c = c.getSuperclass();
                            }
                        }
                    }
                }
                if (localGetter == null) {
                    throw new IllegalArgumentException("There is no suitable accessor for '"
                            + name + "' on class '" + clazz + "'");
                }
                parent = localGetter;
            }
            getter = parent;
            if (getter.isCacheable()) {
                Getter foundGetter = GETTER_CACHE.putIfAbsent(cacheKey, getter);
                if (foundGetter != null) {
                    getter = foundGetter;
                }
            }
            return getter;
        } catch (Throwable e) {
            throw new QueryException(e);
        }
    }

    public static Comparable extractValue(QueryEntry queryEntry, String attributeName, Object object) throws Exception {
        return (Comparable) createGetter(queryEntry, attributeName).getValue(object);
    }

    private abstract static class Getter {
        protected final Getter parent;

        public Getter(final Getter parent) {
            this.parent = parent;
        }

        abstract Object getValue(Object obj) throws Exception;

        abstract Class getReturnType();

        abstract boolean isCacheable();
    }

    static class MethodGetter extends Getter {
        final Method method;

        MethodGetter(Getter parent, Method method) {
            super(parent);
            this.method = method;
        }

        Object getValue(Object obj) throws Exception {
            Object paramObj = obj;
            paramObj = parent != null ? parent.getValue(paramObj) : paramObj;
            return paramObj != null ? method.invoke(paramObj) : null;
        }

        Class getReturnType() {
            return this.method.getReturnType();
        }

        @Override
        boolean isCacheable() {
            return THIS_CL.equals(method.getDeclaringClass().getClassLoader());
        }

        @Override
        public String toString() {
            return "MethodGetter [parent=" + parent + ", method=" + method.getName() + "]";
        }
    }

    static class FieldGetter extends Getter {
        final Field field;

        FieldGetter(Getter parent, Field field) {
            super(parent);
            this.field = field;
        }

        @Override
        Object getValue(Object obj) throws Exception {
            Object paramObj = obj;
            paramObj = parent != null ? parent.getValue(paramObj) : paramObj;
            return paramObj != null ? field.get(paramObj) : null;
        }

        @Override
        Class getReturnType() {
            return this.field.getType();
        }

        @Override
        boolean isCacheable() {
            return THIS_CL.equals(field.getDeclaringClass().getClassLoader());
        }

        @Override
        public String toString() {
            return "FieldGetter [parent=" + parent + ", field=" + field + "]";
        }
    }

    static class ThisGetter extends Getter {
        final Object object;

        public ThisGetter(final Getter parent, Object object) {
            super(parent);
            this.object = object;
        }

        @Override
        Object getValue(Object obj) throws Exception {
            return obj;
        }

        @Override
        Class getReturnType() {
            return this.object.getClass();
        }

        @Override
        boolean isCacheable() {
            return false;
        }
    }
}
