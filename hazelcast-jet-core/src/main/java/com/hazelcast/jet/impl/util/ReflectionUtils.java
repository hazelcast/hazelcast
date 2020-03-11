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

package com.hazelcast.jet.impl.util;

import com.hazelcast.internal.nio.ClassLoaderUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

public final class ReflectionUtils {

    private ReflectionUtils() {
    }

    public static <T> T readStaticFieldOrNull(String classname, String fieldName) {
        try {
            Class<?> clazz = Class.forName(classname);
            return readStaticField(clazz, fieldName);
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException | SecurityException e) {
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
                .enableClassInfo()
                .ignoreClassVisibility();
        try (ScanResult scanResult = classGraph.scan()) {
            Collection<Class<?>> classes = scanResult.getAllClasses()
                                                     .stream()
                                                     .map(ClassInfo::loadClass)
                                                     .collect(toList());
            Collection<URL> nonClasses = scanResult.getAllResources().nonClassFilesOnly().getURLs();
            return new Resources(classes, nonClasses);
        }
    }

    private static String toPath(String packageName) {
        return packageName.replace('.', '/');
    }

    public static Class<?> loadClass(ClassLoader classLoader, String name) {
        try {
            return ClassLoaderUtil.loadClass(classLoader, name);
        } catch (ClassNotFoundException e) {
            throw sneakyThrow(e);
        }
    }

    public static <T> T newInstance(ClassLoader classLoader, String name) {
        try {
            return ClassLoaderUtil.newInstance(classLoader, name);
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    public static final class Resources {

        private final Collection<Class<?>> classes;
        private final Collection<URL> nonClasses;

        private Resources(Collection<Class<?>> classes, Collection<URL> nonClasses) {
            this.classes = classes;
            this.nonClasses = nonClasses;
        }

        public Stream<Class<?>> classes() {
            return classes.stream();
        }

        public Stream<URL> nonClasses() {
            return nonClasses.stream();
        }
    }
}
