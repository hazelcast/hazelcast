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

package com.hazelcast.jet.impl.util;

import java.lang.reflect.Field;

public final class ReflectionUtils {
    private ReflectionUtils() {

    }

    public static <T> T readStaticFieldOrNull(String classname, String fieldName) {
        try {
            Class<?> clazz = Class.forName(classname);
            return readStaticField(clazz, fieldName);
        } catch (ClassNotFoundException e) {
            return null;
        } catch (NoSuchFieldException e) {
            return null;
        } catch (IllegalAccessException e) {
            return null;
        } catch (SecurityException e) {
            return null;
        }
    }

    private static <T> T readStaticField(Class<?> clazz, String fieldName) throws NoSuchFieldException,
            IllegalAccessException {
        Field field = clazz.getDeclaredField(fieldName);
        if (!field.isAccessible()) {
            field.setAccessible(true);
        }
        return (T) field.get(null);
    }
}
