/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.util;

import java.lang.reflect.Field;

public final class ReflectionUtil {

    private ReflectionUtil() {
    }

    /**
     * Finds the value for a static field. If the field doesn't exist, null is returned.
     *
     * @param clazz     the class containing the static field.
     * @param fieldName the name of the static field.
     * @param <E>
     * @return the value of the static field. If the field doesn't exist, null is returned.
     */
    public static <E> E findStaticFieldValue(Class clazz, String fieldName) {
        try {
            Field field = clazz.getField(fieldName);
            return (E) field.get(null);
        } catch (Exception ignore) {
            return null;
        }
    }

    /**
     * Finds the value for a static field. If the field doesn't exist, null is returned.
     *
     * @param className name of the class.
     * @param fieldName the name of the static field.
     * @param <E>
     * @return the value of the static field. If the field doesn't exist, null is returned.
     */
    public static <E> E findStaticFieldValue(String className, String fieldName) {
        try {
            Class<?> clazz = Class.forName(className);
            Field field = clazz.getField(fieldName);
            return (E) field.get(null);
        } catch (Exception ignore) {
            return null;
        }
    }
}
