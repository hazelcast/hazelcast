/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio;

import com.hazelcast.util.ConcurrentReferenceHashMap;
import com.hazelcast.util.ConcurrentReferenceHashMap.ReferenceType;
import com.hazelcast.util.ValidationUtil;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public final class ClassLoaderUtil {

    public static final String HAZELCAST_BASE_PACKAGE = "com.hazelcast.";
    public static final String HAZELCAST_ARRAY = "[L" + HAZELCAST_BASE_PACKAGE;

    private static final Map<String, Class> PRIMITIVE_CLASSES;
    private static final int MAX_PRIM_CLASSNAME_LENGTH = 7;

    private static final ClassCache CLASS_CACHE = new ClassCache();

    static {
        final Map<String, Class> primitives = new HashMap<String, Class>(10, 1.0f);
        primitives.put("boolean", boolean.class);
        primitives.put("byte", byte.class);
        primitives.put("int", int.class);
        primitives.put("long", long.class);
        primitives.put("short", short.class);
        primitives.put("float", float.class);
        primitives.put("double", double.class);
        primitives.put("char", char.class);
        primitives.put("void", void.class);
        PRIMITIVE_CLASSES = Collections.unmodifiableMap(primitives);
    }

    private ClassLoaderUtil() {
    }

    public static <T> T newInstance(ClassLoader classLoader, final String className)
            throws Exception {
        Class<?> klass = loadClass(classLoader, className);
        return (T)newInstance(klass, classLoader, className);
    }

    public static <T> T newInstance(Class<T> klass, ClassLoader classLoader, String className)
            throws Exception {
        final Constructor<T> constructor = klass.getDeclaredConstructor();
        if (!constructor.isAccessible()) {
            constructor.setAccessible(true);
        }
        return constructor.newInstance();
    }

    public static Class<?> loadClass(final ClassLoader classLoader, final String className)
            throws ClassNotFoundException {

        ValidationUtil.isNotNull(className, "className");
        if (className.length() <= MAX_PRIM_CLASSNAME_LENGTH && Character.isLowerCase(className.charAt(0))) {
            final Class primitiveClass = PRIMITIVE_CLASSES.get(className);
            if (primitiveClass != null) {
                return primitiveClass;
            }
        }
        ClassLoader theClassLoader = classLoader;
        if (theClassLoader == null) {
            theClassLoader = Thread.currentThread().getContextClassLoader();
        }

        Class< ? > cachedClass = CLASS_CACHE.get(theClassLoader, className);
        if (cachedClass != null)
        {
            return cachedClass;
        }

        // First try to load it through the given classloader
        if (theClassLoader != null) {
            try {
                final Class< ? > loadedClass = tryLoadClass(className, theClassLoader);
                CLASS_CACHE.put(theClassLoader, className, loadedClass);
                return loadedClass;
            } catch (ClassNotFoundException ignore) {

                // Reset selected classloader and try with others
                theClassLoader = null;
            }
        }

        // If failed and this is a Hazelcast class try again with our classloader
        if (className.startsWith(HAZELCAST_BASE_PACKAGE) || className.startsWith(HAZELCAST_ARRAY)) {
            theClassLoader = ClassLoaderUtil.class.getClassLoader();
        }
        if (theClassLoader == null) {
            theClassLoader = Thread.currentThread().getContextClassLoader();
        }
        if (theClassLoader != null) {
            Class< ? > cachedClass2 = CLASS_CACHE.get(theClassLoader, className);
            if (cachedClass2 != null)
            {
                return cachedClass2;
            }

            final Class< ? > loadedClass = tryLoadClass(className, theClassLoader);
            CLASS_CACHE.put(theClassLoader, className, loadedClass);
            return loadedClass;
        }
        return Class.forName(className);
    }

    private static Class<?> tryLoadClass(String className, ClassLoader classLoader)
            throws ClassNotFoundException {

        if (className.startsWith("[")) {
            return Class.forName(className, false, classLoader);
        } else {
            return classLoader.loadClass(className);
        }
    }

    public static boolean isInternalType(Class type) {
        String name = type.getName();
        ClassLoader classLoader = ClassLoaderUtil.class.getClassLoader();
        return type.getClassLoader() == classLoader && name.startsWith(HAZELCAST_BASE_PACKAGE);
    }

    private static final class ClassCache {
        private final ConcurrentMap<ClassLoader, ConcurrentMap<String, Class<?>>> cache;

        protected ClassCache() {
            // Guess 16 classloaders to not waste to much memory (16 is default concurrency level)
            cache = new ConcurrentReferenceHashMap<ClassLoader, ConcurrentMap<String, Class<?>>>(16,
                            ReferenceType.SOFT, ReferenceType.SOFT);
        }

        protected <T> Class<?> put(ClassLoader classLoader, String className, Class<T> clazz) {
            ClassLoader cl = classLoader == null ? ClassLoaderUtil.class.getClassLoader() : classLoader;
            ConcurrentMap<String, Class<?>> innerCache = cache.get(cl);
            if (innerCache == null) {
                // Let's guess a start of 100 classes per classloader
                innerCache = new ConcurrentHashMap<String, Class<?>>(100);
                ConcurrentMap<String, Class<?>> old = cache.putIfAbsent(cl, innerCache);
                if (old != null) {
                    innerCache = old;
                }
            }
            innerCache.put(className, clazz);
            return clazz;
        }

        protected <T> Class<?> get(ClassLoader classLoader, String className) {
            ConcurrentMap<String, Class<?>> innerCache = cache.get(classLoader);
            if (innerCache == null) {
                return null;
            }
            return innerCache.get(className);
        }
    }
}
