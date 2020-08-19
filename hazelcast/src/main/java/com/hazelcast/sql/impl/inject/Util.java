/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.inject;

import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.sql.impl.QueryException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static java.lang.Character.toUpperCase;

public final class Util {

    private static final String METHOD_PREFIX_SET = "set";

    private Util() {
    }

    public static Class<?> loadClass(String className) {
        try {
            return ClassLoaderUtil.loadClass(null, className);
        } catch (ClassNotFoundException e) {
            throw sneakyThrow(e);
        }
    }

    public static Method extractSetter(Class<?> clazz, String propertyName, Class<?> type) {
        String setName = METHOD_PREFIX_SET + toUpperCase(propertyName.charAt(0)) + propertyName.substring(1);

        Method method;
        try {
            method = clazz.getMethod(setName, type);
        } catch (NoSuchMethodException e) {
            return null;
        }

        if (!isSetter(method)) {
            return null;
        }

        return method;
    }

    @SuppressWarnings("RedundantIfStatement")
    public static boolean isSetter(Method method) {
        if (!Modifier.isPublic(method.getModifiers())) {
            return false;
        }

        if (Modifier.isStatic(method.getModifiers())) {
            return false;
        }

        Class<?> returnType = method.getReturnType();
        if (returnType != void.class && returnType != Void.class) {
            return false;
        }

        return true;
    }

    public static Field extractField(Class<?> clazz, String fieldName) {
        Field field;
        try {
            field = clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            return null;
        }

        if (skipField(field)) {
            return null;
        }

        return field;
    }

    @SuppressWarnings("RedundantIfStatement")
    public static boolean skipField(Field field) {
        if (!Modifier.isPublic(field.getModifiers())) {
            return true;
        }

        return false;
    }

    public static ClassDefinition lookupClassDefinition(
            InternalSerializationService serializationService,
            int factoryId,
            int classId,
            int classVersion
    ) {
        ClassDefinition classDefinition = serializationService
                .getPortableContext()
                .lookupClassDefinition(factoryId, classId, classVersion);
        if (classDefinition == null) {
            throw QueryException.dataException(
                    "Unable to find class definition for factoryId: " + factoryId
                            + ", classId: " + classId + ", classVersion: " + classVersion
            );
        }
        return classDefinition;
    }
}
