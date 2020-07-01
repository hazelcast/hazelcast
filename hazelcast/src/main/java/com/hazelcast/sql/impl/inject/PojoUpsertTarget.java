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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.sql.impl.schema.map.options.JavaMapOptionsMetadataResolver.extractField;
import static com.hazelcast.sql.impl.schema.map.options.JavaMapOptionsMetadataResolver.extractSetter;
import static com.hazelcast.sql.impl.schema.map.options.JavaMapOptionsMetadataResolver.loadClass;
import static java.lang.String.format;
import static java.util.stream.Collectors.toMap;

// TODO: can it be non-thread safe ?
public class PojoUpsertTarget implements UpsertTarget {

    private final Class<?> clazz;
    private final Map<String, Class<?>> typesByPaths;

    private Object pojo;

    PojoUpsertTarget(String className, Map<String, String> typeNamesByPaths) {
        this.clazz = loadClass(className);
        this.typesByPaths = typeNamesByPaths.entrySet().stream()
                                             .collect(toMap(Entry::getKey, entry -> loadClass(entry.getValue())));
    }

    @Override
    public UpsertInjector createInjector(String path) {
        Method method = extractSetter(clazz, path, typesByPaths.get(path));
        if (method != null) {
            return createMethodInjector(method, path);
        } else {
            Field field = extractField(clazz, path);
            return createFieldInjector(field, path);
        }
    }

    private UpsertInjector createMethodInjector(Method method, String path) {
        return value -> {
            if (value != null) {
                if (method == null) {
                    throw QueryException.dataException(
                            format("Unable to inject non null (%s) '%s' into %s", value, path, clazz.getName())
                    );
                }

                try {
                    method.invoke(pojo, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw QueryException.dataException(
                            format("Cannot inject field \"%s\" into %s : %s", path, clazz.getName(), e.getMessage()), e
                    );
                }
            }
        };
    }

    private UpsertInjector createFieldInjector(Field field, String path) {
        return value -> {
            if (value != null) {
                if (field == null) {
                    throw QueryException.dataException(
                            format("Unable to inject non null (%s) '%s' into %s", value, path, clazz.getName())
                    );
                }

                try {
                    field.set(pojo, value);
                } catch (IllegalAccessException e) {
                    throw QueryException.dataException(
                            format("Cannot inject field \"%s\" into %s : %s", path, clazz.getName(), e.getMessage()), e
                    );
                }
            }
        };
    }

    @Override
    public void init() {
        try {
            pojo = clazz.newInstance();
        } catch (Exception e) {
            throw QueryException.dataException(
                    format("Unable to instantiate class \"%s\" : %s", clazz.getName(), e.getMessage()), e
            );
        }
    }

    @Override
    public Object conclude() {
        Object pojo = this.pojo;
        this.pojo = null;
        return pojo;
    }
}
