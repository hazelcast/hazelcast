/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.jet.sql.impl.type.converter.ToConverters;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.UnaryOperator;

import static com.hazelcast.jet.impl.util.ReflectionUtils.loadClass;
import static com.hazelcast.jet.impl.util.ReflectionUtils.newInstance;
import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;

@NotThreadSafe
class PojoUpsertTarget extends UpsertTarget {
    private final Class<?> typeClass;

    private Object object;

    PojoUpsertTarget(Class<?> typeClass, InternalSerializationService serializationService) {
        super(serializationService);
        this.typeClass = typeClass;
    }

    @Override
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            if (type.isCustomType()) {
                UnaryOperator<Object> converter = customTypeConverter(type);
                return value -> object = converter.apply(value);
            } else {
                return FAILING_TOP_LEVEL_INJECTOR;
            }
        }
        Injector<Object> injector = createInjector(typeClass, path, type);
        return value -> injector.set(object, value);
    }

    protected Injector<Object> createInjector(Class<?> typeClass, String path, QueryDataType type) {
        UnaryOperator<Object> converter = type.isCustomType()
                ? customTypeConverter(type)
                : ToConverters.getToConverter(type)::convert;

        Method method = ReflectionUtils.findPropertySetter(typeClass, path);
        if (method != null) {
            return (object, value) -> {
                if (value == null && method.getParameterTypes()[0].isPrimitive()) {
                    throw QueryException.error("Cannot pass NULL to a method with a primitive argument: " + method);
                }
                try {
                    method.invoke(object, converter.apply(value));
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw QueryException.error("Invocation of '" + method + "' failed: " + e, e);
                }
            };
        }

        Field field = ReflectionUtils.findPropertyField(typeClass, path);
        if (field != null) {
            return (object, value) -> {
                if (value == null && field.getType().isPrimitive()) {
                    throw QueryException.error("Cannot set NULL to a primitive field: " + field);
                }
                try {
                    field.set(object, converter.apply(value));
                } catch (IllegalAccessException e) {
                    throw QueryException.error("Failed to set field " + field + ": " + e, e);
                }
            };
        }

        return (object, value) -> {
            if (value != null) {
                throw QueryException.error("Cannot set property \"" + path + "\" to class "
                        + typeClass.getName() + ": no set-method or public field available");
            }
        };
    }

    protected UnaryOperator<Object> customTypeConverter(QueryDataType type) {
        Class<?> typeClass = loadClass(type.getObjectTypeMetadata());
        Injector<Object> injector = createRecordInjector(type,
                (fieldName, fieldType) -> createInjector(typeClass, fieldName, fieldType));
        return value -> {
            if (value == null || typeClass.isInstance(value)) {
                return value;
            }
            Object object = createObject(typeClass);
            injector.set(object, value);
            return object;
        };
    }

    protected static Object createObject(Class<?> typeClass) {
        try {
            return newInstance(Thread.currentThread().getContextClassLoader(), typeClass.getName());
        } catch (Exception e) {
            throw QueryException.error("Unable to instantiate class \""
                    + typeClass.getName() + "\" : " + e.getMessage(), e);
        }
    }

    @Override
    public void init() {
        object = createObject(typeClass);
    }

    @Override
    public Object conclude() {
        Object object = this.object;
        this.object = null;
        return object;
    }
}
