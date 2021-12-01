/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.ReflectionUtils.loadClass;
import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;
import static java.util.stream.Collectors.toMap;

@NotThreadSafe
class PojoUpsertTarget implements UpsertTarget {

    private static final String NESTING_SEPARATOR = "\\.";
    private final Class<?> clazz;
    private final Map<String, Class<?>> typesByPaths;

    private Object pojo;

    PojoUpsertTarget(String className, Map<String, String> typeNamesByPaths) {
        this.clazz = loadClass(className);
        this.typesByPaths = typeNamesByPaths.entrySet().stream()
                                            .collect(toMap(Entry::getKey, entry -> loadClass(entry.getValue())));
    }

    @Override
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            return FAILING_TOP_LEVEL_INJECTOR;
        }

        if (isNestedFieldPath(path)) {
            return createNestedInjector(path, type);
        }

        Method method = ReflectionUtils.findPropertySetter(clazz, path, typesByPaths.get(path));
        if (method != null) {
            return createMethodInjector(method);
        } else {
            Field field = ReflectionUtils.findPropertyField(clazz, path);
            if (field != null) {
                return createFieldInjector(field);
            } else {
                return value -> {
                    System.out.println("injecting " + path);
                };
            }
        }
    }

    private UpsertInjector createNestedInjector(String path, QueryDataType type) {
        final List<String> fields = Arrays.asList(path.split(NESTING_SEPARATOR));

        final List<String> objFields = fields.subList(0, fields.size() - 1);
        final String finalFieldName = fields.get(fields.size() - 1);
        final List<Function<Object, Object>> subPathAccessors = new ArrayList<>();

        Class<?> currentClass = clazz;
        for (final String fieldName : objFields) {
            final Method getter = ReflectionUtils.findPropertyGetter(currentClass, fieldName);
            if (getter != null) {
                subPathAccessors.add(o -> {
                    try {
                        return getter.invoke(o);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        throw QueryException.error("Invocation of '" + getter + "' failed: " + e, e);
                    }
                });
                currentClass = getter.getReturnType();
            } else {
                final Field field = ReflectionUtils.findPropertyField(currentClass, fieldName);
                if (field == null) {
                    throw QueryException.error("Failed to find accessors for field " + fieldName
                            + " within path: " + path);
                }
                subPathAccessors.add(o -> {
                    try {
                        return field.get(o);
                    } catch (IllegalAccessException e) {
                        throw QueryException.error("Failed to set field " + field + ": " + e, e);
                    }
                });
                currentClass = field.getType();
            }
        }

        // TODO: deduplicate
        Consumer<Object> setter;
        final Method method = ReflectionUtils.findPropertySetter(currentClass, finalFieldName, typesByPaths.get(path));
        if (method != null) {
            setter = value -> {
                if (value == null && method.getParameterTypes()[0].isPrimitive()) {
                    throw QueryException.error("Cannot pass NULL to a method with a primitive argument: " + method);
                }

                try {
                    Object currentObject = pojo;
                    for (final Function<Object, Object> accessor : subPathAccessors) {
                        currentObject = accessor.apply(currentObject);
                    }

                    // TODO: fix the sub-field assignment
                    if (currentObject == null) {
                        return;
                    }

                    method.invoke(currentObject, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw QueryException.error("Invocation of '" + method + "' failed: " + e, e);
                } catch (NullPointerException e) {
                    System.out.println("hello");
                }
            };
        } else {
            final Field field = ReflectionUtils.findPropertyField(clazz, path);
            if (field == null) {
                throw QueryException.error("Failed to create field injector for field " + path);
            }
            setter = value -> {
                if (value == null && field.getType().isPrimitive()) {
                    throw QueryException.error("Cannot set NULL to a primitive field: " + field);
                }

                Object currentObject = pojo;
                for (final Function<Object, Object> accessor : subPathAccessors) {
                    currentObject = accessor.apply(currentObject);
                }

                try {
                    field.set(currentObject, value);
                } catch (IllegalAccessException e) {
                    throw QueryException.error("Failed to set field " + field + ": " + e, e);
                }
            };
        }

        return setter::accept;
    }

    // We might change convention later on.
    private boolean isNestedFieldPath(String path) {
        return path.contains(".");
    }

    private UpsertInjector createMethodInjector(@Nonnull Method method) {
        return value -> {
            if (value == null && method.getParameterTypes()[0].isPrimitive()) {
                throw QueryException.error("Cannot pass NULL to a method with a primitive argument: " + method);
            }
            try {
                method.invoke(pojo, value);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw QueryException.error("Invocation of '" + method + "' failed: " + e, e);
            }
        };
    }

    private UpsertInjector createFieldInjector(@Nonnull Field field) {
        return value -> {
            if (value == null && field.getType().isPrimitive()) {
                throw QueryException.error("Cannot set NULL to a primitive field: " + field);
            }
            try {
                field.set(pojo, value);
            } catch (IllegalAccessException e) {
                throw QueryException.error("Failed to set field " + field + ": " + e, e);
            }
        };
    }

    @Nonnull
    private UpsertInjector createFailingInjector(String path) {
        return value -> {
            if (value != null) {
                throw QueryException.error("Cannot set property \"" + path + "\" to class " + clazz.getName()
                        + ": no set-method or public field available");
            }
        };
    }

    @Override
    public void init() {
        try {
            pojo = clazz.newInstance();
        } catch (Exception e) {
            throw QueryException.error("Unable to instantiate class \"" + clazz.getName() + "\" : "
                    + e.getMessage(), e);
        }
    }

    @Override
    public Object conclude() {
        Object pojo = this.pojo;
        this.pojo = null;
        return pojo;
    }
}
