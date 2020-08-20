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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.sql.impl.inject.Util.extractField;
import static com.hazelcast.sql.impl.inject.Util.extractSetter;
import static com.hazelcast.sql.impl.inject.Util.loadClass;
import static java.util.stream.Collectors.toMap;

class JavaUpsertTarget implements UpsertTarget {

    private final Class<?> clazz;
    private final Map<String, Class<?>> typesByPaths;

    private Object object;

    JavaUpsertTarget(String className, Map<String, String> typeNamesByPaths) {
        Class<?> clazz = loadClass(className);
        QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(clazz);

        this.clazz = type == QueryDataType.OBJECT ? clazz : null;
        this.typesByPaths = typeNamesByPaths.entrySet().stream()
                                            .collect(toMap(Entry::getKey, entry -> loadClass(entry.getValue())));
    }

    @Override
    public UpsertInjector createInjector(String path) {
        if (path == null) {
            assert clazz == null;

            return createObjectInjector();
        } else {
            assert clazz != null;

            Method method = extractSetter(clazz, path, typesByPaths.get(path));
            if (method != null) {
                return createMethodInjector(method, path);
            } else {
                Field field = extractField(clazz, path);
                return createFieldInjector(field, path);
            }
        }
    }

    private UpsertInjector createObjectInjector() {
        return value -> {
            assert object == null;

            object = value;
        };
    }

    private UpsertInjector createMethodInjector(Method method, String path) {
        return value -> {
            assert object != null;

            if (value != null) {
                if (method == null) {
                    throw QueryException.dataException(
                            "Unable to inject non null (" + value + ") '" + path + "' into " + clazz.getName()
                    );
                }

                try {
                    method.invoke(object, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw QueryException.dataException(
                            "Cannot inject field \"" + path + "\" into " + clazz.getName() + " : " + e.getMessage(), e
                    );
                }
            }
        };
    }

    private UpsertInjector createFieldInjector(Field field, String path) {
        return value -> {
            assert object != null;

            if (value != null) {
                if (field == null) {
                    throw QueryException.dataException(
                            "Unable to inject non null (" + value + ") '" + path + "' into " + clazz.getName()
                    );
                }

                try {
                    field.set(object, value);
                } catch (IllegalAccessException e) {
                    throw QueryException.dataException(
                            "Cannot inject field \"" + path + "\" into " + clazz.getName() + " : " + e.getMessage(), e
                    );
                }
            }
        };
    }

    @Override
    public void init() {
        if (clazz != null) {
            try {
                object = clazz.newInstance();
            } catch (Exception e) {
                throw QueryException.dataException(
                        "Unable to instantiate class \"" + clazz.getName() + "\" : " + e.getMessage(), e
                );
            }
        }
    }

    @Override
    public Object conclude() {
        Object object = this.object;
        this.object = null;
        return object;
    }
}
