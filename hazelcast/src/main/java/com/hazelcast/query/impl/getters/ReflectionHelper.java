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

import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.AttributeType;
import com.hazelcast.query.impl.IndexImpl;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;
import static com.hazelcast.query.impl.getters.NullGetter.NULL_GETTER;

/**
 * Scans your classpath, indexes the metadata, allows you to query it on runtime.
 */
public final class ReflectionHelper {
    static final ClassLoader THIS_CL = ReflectionHelper.class.getClassLoader();

    private static final int INITIAL_CAPACITY = 3;

    private static final ConcurrentMap<Class, ConcurrentMap<String, Getter>> GETTER_CACHE
            = new ConcurrentHashMap<Class, ConcurrentMap<String, Getter>>(1000);

    private static final ConstructorFunction<Class, ConcurrentMap<String, Getter>> GETTER_CACHE_CONSTRUCTOR
            = new ConstructorFunction<Class, ConcurrentMap<String, Getter>>() {
        @Override
        public ConcurrentMap<String, Getter> createNew(Class arg) {
            return new ConcurrentHashMap<String, Getter>();
        }
    };


    // we don't want instances
    private ReflectionHelper() {
    }

    public static AttributeType getAttributeType(Class klass) {
        if (klass == null) {
            return null;
        }

        if (klass == String.class) {
            return AttributeType.STRING;
        } else if (klass == int.class || klass == Integer.class) {
            return AttributeType.INTEGER;
        } else if (klass == short.class || klass == Short.class) {
            return AttributeType.SHORT;
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
        } else if (klass == UUID.class) {
            return AttributeType.UUID;
        }
        return null;
    }


    private static Getter getFromCache(Class clazz, String attribute) {
        ConcurrentMap<String, Getter> cache = GETTER_CACHE.get(clazz);
        if (cache == null) {
            return null;
        }

        return cache.get(attribute);
    }

    private static Getter storeIntoCache(Class clazz, String attribute, Getter getter) {
        ConcurrentMap<String, Getter> cache = ConcurrencyUtil.getOrPutIfAbsent(GETTER_CACHE, clazz, GETTER_CACHE_CONSTRUCTOR);
        Getter foundGetter = cache.putIfAbsent(attribute, getter);
        return foundGetter == null ? getter : foundGetter;
    }

    public static void reset() {
        GETTER_CACHE.clear();
    }

    public static AttributeType getAttributeType(Object value, String attribute) {
        Getter getter = createGetter(value, attribute);
        Class returnType = getter.getReturnType();
        return getAttributeType(returnType);
    }

    private static Getter createGetter(Object obj, String attribute) {
        if (obj == null || obj == IndexImpl.NULL) {
            return NULL_GETTER;
        }

        final Class targetClazz = obj.getClass();
        Class clazz = targetClazz;
        Getter getter = getFromCache(clazz, attribute);
        if (getter != null) {
            return getter;
        }

        try {
            Getter parent = null;
            List<String> possibleMethodNames = new ArrayList<String>(INITIAL_CAPACITY);
            for (final String fullname : attribute.split("\\.")) {
                String nameWithoutSuffix = removeReducerSuffix(fullname);
                String suffix = (nameWithoutSuffix == fullname) ? null : getReducerSuffix(fullname);

                Getter localGetter = null;
                possibleMethodNames.clear();
                possibleMethodNames.add(nameWithoutSuffix);
                final String camelName = Character.toUpperCase(nameWithoutSuffix.charAt(0)) + nameWithoutSuffix.substring(1);
                possibleMethodNames.add("get" + camelName);
                possibleMethodNames.add("is" + camelName);
                if (nameWithoutSuffix.equals(THIS_ATTRIBUTE_NAME)) {
                    localGetter = GetterFactory.newThisGetter(parent, obj);
                } else {

                    if (parent != null) {
                        clazz = parent.getReturnType();
                    }

                    if (localGetter == null) {
                        for (String methodName : possibleMethodNames) {
                            try {
                                final Method method = clazz.getMethod(methodName);
                                method.setAccessible(true);
                                localGetter = GetterFactory.newMethodGetter(parent, method);
                                clazz = method.getReturnType();
                                break;
                            } catch (NoSuchMethodException ignored) {
                                EmptyStatement.ignore(ignored);
                            }
                        }
                    }
                    if (localGetter == null) {
                        try {
                            final Field field = clazz.getField(nameWithoutSuffix);
                            localGetter = GetterFactory.newFieldGetter(obj, parent, field, suffix);
                            if (localGetter == NULL_GETTER) {
                                return localGetter;
                            }
                            clazz = field.getType();
                        } catch (NoSuchFieldException ignored) {
                            EmptyStatement.ignore(ignored);
                        }
                    }
                    if (localGetter == null) {
                        Class c = clazz;
                        while (!c.isInterface() && !Object.class.equals(c)) {
                            try {
                                final Field field = c.getDeclaredField(nameWithoutSuffix);
                                field.setAccessible(true);
                                localGetter = GetterFactory.newFieldGetter(obj, parent, field, suffix);
                                if (localGetter == NULL_GETTER) {
                                    return NULL_GETTER;
                                }
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
                            + nameWithoutSuffix + "' on class '" + clazz + "'");
                }
                parent = localGetter;
            }
            getter = parent;

            if (getter.isCacheable()) {
                getter = storeIntoCache(targetClazz, attribute, getter);
            }
            return getter;
        } catch (Throwable e) {
            throw new QueryException(e);
        }
    }

    private static String removeReducerSuffix(String name) {
        int indexOfOpeningBracket = name.indexOf('[');
        if (indexOfOpeningBracket == -1) {
            return name;
        }
        return name.substring(0, indexOfOpeningBracket);
    }

    private static String getReducerSuffix(String name) {
        int indexOfOpeningBracket = name.indexOf('[');
        return name.substring(indexOfOpeningBracket, name.length());
    }


    public static Object extractValue(Object object, String attributeName) throws Exception {
        return createGetter(object, attributeName).getValue(object);
    }

    public static <T> T invokeMethod(Object object, String methodName) throws RuntimeException {
        try {
            Method method = object.getClass().getMethod(methodName);
            method.setAccessible(true);
            return (T) method.invoke(object);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
