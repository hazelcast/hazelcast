/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.nio.ClassLoaderUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static java.lang.Character.toUpperCase;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

public final class ReflectionUtils {

    private ReflectionUtils() {
    }

    /**
     * Load a class using the current thread's context class loader as a
     * classLoaderHint. Exceptions are sneakily thrown.
     */
    public static Class<?> loadClass(String name) {
        return loadClass(Thread.currentThread().getContextClassLoader(), name);
    }

    /**
     * See {@link ClassLoaderUtil#loadClass(ClassLoader, String)}. Exceptions
     * are sneakily thrown.
     */
    public static Class<?> loadClass(ClassLoader classLoaderHint, String name) {
        try {
            return ClassLoaderUtil.loadClass(classLoaderHint, name);
        } catch (ClassNotFoundException e) {
            throw sneakyThrow(e);
        }
    }

    /**
     * See {@link ClassLoaderUtil#newInstance(ClassLoader, String)}. Exceptions
     * are sneakily thrown.
     */
    public static <T> T newInstance(ClassLoader classLoader, String name) {
        try {
            return ClassLoaderUtil.newInstance(classLoader, name);
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    /**
     * Reads a value of a static field. In case of any exceptions it returns
     * null.
     */
    public static <T> T readStaticFieldOrNull(String className, String fieldName) {
        try {
            Class<?> clazz = Class.forName(className);
            return readStaticField(clazz, fieldName);
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException | SecurityException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T readStaticField(Class<?> clazz, String fieldName) throws NoSuchFieldException,
            IllegalAccessException {
        Field field = clazz.getDeclaredField(fieldName);
        if (!field.isAccessible()) {
            field.setAccessible(true);
        }
        return (T) field.get(null);
    }

    /**
     * Return a set-method for a class and a property. The setter must start
     * with "set", must be public, non-static, must return void or the
     * containing class type (a builder-style setter) and take one argument of
     * {@code propertyType}.
     *
     * @param clazz The containing class
     * @param propertyName Name of the property
     * @param propertyType The propertyType of the property
     *
     * @return The found setter or null if one matching the criteria doesn't exist
     */
    @Nullable
    public static Method findPropertySetter(
            @Nonnull Class<?> clazz,
            @Nonnull String propertyName,
            @Nonnull Class<?> propertyType
    ) {
        String setterName = "set" + toUpperCase(propertyName.charAt(0)) + propertyName.substring(1);

        Method method;
        try {
            method = clazz.getMethod(setterName, propertyType);
        } catch (NoSuchMethodException e) {
            return null;
        }

        if (!Modifier.isPublic(method.getModifiers())) {
            return null;
        }

        if (Modifier.isStatic(method.getModifiers())) {
            return null;
        }

        Class<?> returnType = method.getReturnType();
        if (returnType != void.class && returnType != Void.class && returnType != clazz) {
            return null;
        }

        return method;
    }

    /**
     * Return a {@link Field} object for the given {@code fieldName}. The field
     * must be public and non-static.
     *
     * @param clazz The containing class
     * @param fieldName The field
     * @return The field object or null, if not found or doesn't match the criteria.
     */
    @Nullable
    public static Field findPropertyField(Class<?> clazz, String fieldName) {
        Field field;
        try {
            field = clazz.getField(fieldName);
        } catch (NoSuchFieldException e) {
            return null;
        }

        if (!Modifier.isPublic(field.getModifiers()) || Modifier.isStatic(field.getModifiers())) {
            return null;
        }

        return field;
    }

    @Nonnull
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", justification =
            "False positive on try-with-resources as of JDK11")
    public static Collection<Class<?>> nestedClassesOf(Class<?>... classes) {
        ClassGraph classGraph = new ClassGraph()
                .enableClassInfo()
                .ignoreClassVisibility();
        stream(classes).map(Class::getClassLoader).distinct().forEach(classGraph::addClassLoader);
        stream(classes).map(ReflectionUtils::toPackageName).distinct().forEach(classGraph::whitelistPackages);
        try (ScanResult scanResult = classGraph.scan()) {
            Set<String> classNames = stream(classes).map(Class::getName).collect(toSet());
            return concat(
                    stream(classes),
                    scanResult.getAllClasses()
                              .stream()
                              .filter(classInfo -> classNames.contains(classInfo.getName()))
                              .flatMap(classInfo -> classInfo.getInnerClasses().stream())
                              .map(ClassInfo::loadClass)
            ).collect(toList());
        }
    }

    private static String toPackageName(Class<?> clazz) {
        return Optional.ofNullable(clazz.getPackage()).map(Package::getName).orElse("");
    }

    @Nonnull
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", justification =
            "False positive on try-with-resources as of JDK11")
    public static Resources resourcesOf(String... packages) {
        String[] paths = stream(packages).map(ReflectionUtils::toPath).toArray(String[]::new);
        ClassGraph classGraph = new ClassGraph()
                .whitelistPackages(packages)
                .whitelistPaths(paths)
                .ignoreClassVisibility();
        try (ScanResult scanResult = classGraph.scan()) {
            Collection<ClassResource> classes =
                    scanResult.getAllClasses().stream().map(ClassResource::new).collect(toList());
            Collection<URL> nonClasses = scanResult.getAllResources().nonClassFilesOnly().getURLs();
            return new Resources(classes, nonClasses);
        }
    }

    private static String toPath(String name) {
        return name.replace('.', '/');
    }

    public static String toClassResourceId(String name) {
        return toPath(name) + ".class";
    }

    public static final class Resources {

        private final Collection<ClassResource> classes;
        private final Collection<URL> nonClasses;

        private Resources(Collection<ClassResource> classes, Collection<URL> nonClasses) {
            this.classes = classes;
            this.nonClasses = nonClasses;
        }

        public Stream<ClassResource> classes() {
            return classes.stream();
        }

        public Stream<URL> nonClasses() {
            return nonClasses.stream();
        }
    }

    public static final class ClassResource {

        private final String id;
        private final URL url;

        private ClassResource(ClassInfo classInfo) {
            this(classInfo.getName(), classInfo.getResource().getURL());
        }

        public ClassResource(String name, URL url) {
            this.id = toClassResourceId(name);
            this.url = url;
        }

        public String getId() {
            return id;
        }

        public URL getUrl() {
            return url;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClassResource that = (ClassResource) o;
            return Objects.equals(id, that.id) &&
                    Objects.equals(url, that.url);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, url);
        }
    }
}
