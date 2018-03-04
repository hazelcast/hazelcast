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

import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.AttributeType;
import com.hazelcast.query.impl.IndexImpl;
import com.hazelcast.util.ExceptionUtil;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;
import static com.hazelcast.query.impl.getters.NullGetter.NULL_GETTER;
import static com.hazelcast.query.impl.getters.NullMultiValueGetter.NULL_MULTIVALUE_GETTER;
import static com.hazelcast.query.impl.getters.SuffixModifierUtils.getModifierSuffix;
import static com.hazelcast.query.impl.getters.SuffixModifierUtils.removeModifierSuffix;
import static com.hazelcast.util.EmptyStatement.ignore;

/**
 * Scans your classpath, indexes the metadata, allows you to query it on runtime.
 */
public final class ReflectionHelper {
    static final ClassLoader THIS_CL = ReflectionHelper.class.getClassLoader();

    private static final int INITIAL_CAPACITY = 3;

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

    public static Getter createGetter(Object obj, String attribute) {
        if (obj == null || obj == IndexImpl.NULL) {
            return NULL_GETTER;
        }

        final Class targetClazz = obj.getClass();
        Class clazz = targetClazz;
        Getter getter;

        try {
            Getter parent = null;
            List<String> possibleMethodNames = new ArrayList<String>(INITIAL_CAPACITY);
            for (final String fullname : attribute.split("\\.")) {
                String baseName = removeModifierSuffix(fullname);
                String modifier = getModifierSuffix(fullname, baseName);

                Getter localGetter = null;
                possibleMethodNames.clear();
                possibleMethodNames.add(baseName);
                final String camelName = Character.toUpperCase(baseName.charAt(0)) + baseName.substring(1);
                possibleMethodNames.add("get" + camelName);
                possibleMethodNames.add("is" + camelName);
                if (baseName.equals(THIS_ATTRIBUTE_NAME.value())) {
                    localGetter = GetterFactory.newThisGetter(parent, obj);
                } else {

                    if (parent != null) {
                        clazz = parent.getReturnType();
                    }

                    for (String methodName : possibleMethodNames) {
                        try {
                            final Method method = clazz.getMethod(methodName);
                            method.setAccessible(true);
                            localGetter = GetterFactory.newMethodGetter(obj, parent, method, modifier);
                            if (localGetter == NULL_GETTER || localGetter == NULL_MULTIVALUE_GETTER) {
                                return localGetter;
                            }
                            clazz = method.getReturnType();
                            break;
                        } catch (NoSuchMethodException ignored) {
                            ignore(ignored);
                        }
                    }
                    if (localGetter == null) {
                        try {
                            final Field field = clazz.getField(baseName);
                            localGetter = GetterFactory.newFieldGetter(obj, parent, field, modifier);
                            if (localGetter == NULL_GETTER || localGetter == NULL_MULTIVALUE_GETTER) {
                                return localGetter;
                            }
                            clazz = field.getType();
                        } catch (NoSuchFieldException ignored) {
                            ignore(ignored);
                        }
                    }
                    if (localGetter == null) {
                        Class c = clazz;
                        while (!c.isInterface() && !Object.class.equals(c)) {
                            try {
                                final Field field = c.getDeclaredField(baseName);
                                field.setAccessible(true);
                                localGetter = GetterFactory.newFieldGetter(obj, parent, field, modifier);
                                if (localGetter == NULL_GETTER || localGetter == NULL_MULTIVALUE_GETTER) {
                                    return localGetter;
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
                            + baseName + "' on class '" + clazz.getName() + "'");
                }
                parent = localGetter;
            }
            getter = parent;
            return getter;
        } catch (Throwable e) {
            throw new QueryException(e);
        }
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
