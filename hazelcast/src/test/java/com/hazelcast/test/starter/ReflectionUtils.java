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

package com.hazelcast.test.starter;

import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.net.URLClassLoader.newInstance;

/**
 * Reflection utilities.
 */
@SuppressWarnings("WeakerAccess")
public final class ReflectionUtils {

    private ReflectionUtils() {
    }

    public static Reflections getReflectionsForTestPackage(String forPackage) {
        try {
            URL testClassesURL = new File("target/test-classes").toURI().toURL();
            URLClassLoader classLoader = newInstance(new URL[]{testClassesURL}, ClasspathHelper.staticClassLoader());
            return new Reflections(new ConfigurationBuilder()
                    .addUrls(ClasspathHelper.forPackage(forPackage, classLoader))
                    .addClassLoader(classLoader)
                    .filterInputsBy(new FilterBuilder().includePackage(forPackage))
                    .setScanners(
                            new SubTypesScanner(false),
                            new TypeAnnotationsScanner(),
                            new MethodAnnotationsScanner()));
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> Constructor<T> getConstructor(Class<?> constructorClass, Class<?>... parameterTypes) {
        try {
            Constructor<T> constructor = (Constructor<T>) constructorClass.getConstructor(parameterTypes);
            constructor.setAccessible(true);
            return constructor;
        } catch (NoSuchMethodException ignored) {
            try {
                Constructor<T> constructor = (Constructor<T>) constructorClass.getDeclaredConstructor(parameterTypes);
                constructor.setAccessible(true);
                return constructor;
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException("Could not find constructor " + constructorClass.getSimpleName() + "("
                        + Arrays.toString(parameterTypes) + ")", e);
            }
        }
    }

    public static Object getFieldValueReflectively(Object arg, String fieldName) throws IllegalAccessException {
        checkNotNull(arg, "Argument cannot be null");
        checkHasText(fieldName, "Field name cannot be null");

        Field field = getAllFieldsByName(arg.getClass()).get(fieldName);
        if (field == null) {
            throw new NoSuchFieldError("Field " + fieldName + " does not exist on object " + arg);
        }

        field.setAccessible(true);
        return field.get(arg);
    }

    public static void setFieldValueReflectively(Object arg, String fieldName, Object newValue) throws IllegalAccessException {
        checkNotNull(arg, "Argument cannot be null");
        checkHasText(fieldName, "Field name cannot be null");

        Field field = getAllFieldsByName(arg.getClass()).get(fieldName);
        if (field == null) {
            throw new NoSuchFieldError("Field " + fieldName + " does not exist on object " + arg);
        }

        field.setAccessible(true);
        field.set(arg, newValue);
    }

    public static Map<String, Field> getAllFieldsByName(Class<?> clazz) {
        ConcurrentMap<String, Field> fields = new ConcurrentHashMap<String, Field>();
        Field[] ownFields = clazz.getDeclaredFields();
        for (Field field : ownFields) {
            fields.put(field.getName(), field);
        }
        Class<?> superClass = clazz.getSuperclass();
        while (superClass != null) {
            ownFields = superClass.getDeclaredFields();
            for (Field field : ownFields) {
                fields.putIfAbsent(field.getName(), field);
            }
            superClass = superClass.getSuperclass();
        }
        return fields;
    }
}
