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
import com.hazelcast.sql.impl.QueryException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.lang.String.format;

public class PojoUpsertTarget implements UpsertTarget {

    private static final String METHOD_PREFIX_SET = "set";

    private final Class<?> clazz;

    PojoUpsertTarget(String className) {
        try {
            this.clazz = ClassLoaderUtil.tryLoadClass(className);
        } catch (Exception e) {
            throw QueryException.dataException(
                    format("Unable to load class \"%s\" : %s", className, e.getMessage()), e
            );
        }
    }

    @Override
    public TargetHolder get() {
        try {
            return new TargetHolder(clazz.newInstance()); // TODO: reuse ???
        } catch (Exception e) {
            throw QueryException.dataException(
                    format("Unable to instantiate class \"%s\" : %s", clazz.getName(), e.getMessage()), e
            );
        }
    }

    @Override
    public UpsertInjector createInjector(String path) {
        Method method = findMethod(path);
        return (holder, value) -> {
            Object target = checkNotNull(holder.get(), "Missing target");

            if (value != null) {
                if (method == null) {
                    throw QueryException.dataException(
                            format("Unable to inject non null (%s) '%s' into %s", value, path, clazz.getName())
                    );
                }

                try {
                    method.invoke(target, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw QueryException.dataException(
                            format("Cannot inject field \"%s\" into %s : %s", path, clazz.getName(), e.getMessage()), e
                    );
                }
            }
        };
    }

    private Method findMethod(String fieldName) {
        String methodName = METHOD_PREFIX_SET + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        for (Method method : clazz.getMethods()) {
            // TODO: better heuristics ???
            if (method.getName().equalsIgnoreCase(methodName) && method.getParameterCount() == 1) {
                return method;
            }
        }
        return null;
    }
}
