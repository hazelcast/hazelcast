/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.util.ConcurrentReferenceHashMap;
import com.hazelcast.util.EmptyStatement;

import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Utility class to deal with classloaders.
 */
@PrivateApi
public final class ClassLoaderUtil {

    public static final String HAZELCAST_BASE_PACKAGE = "com.hazelcast.";
    public static final String HAZELCAST_ARRAY = "[L" + HAZELCAST_BASE_PACKAGE;

    private static final boolean CLASS_CACHE_DISABLED = Boolean.getBoolean("hazelcast.compat.classloading.cache.disabled");

    private static final Map<String, Class> PRIMITIVE_CLASSES;
    private static final int MAX_PRIM_CLASSNAME_LENGTH = 7;

    private static final ClassLoaderWeakCache<Constructor> CONSTRUCTOR_CACHE = new ClassLoaderWeakCache<Constructor>();
    private static final ClassLoaderWeakCache<Class> CLASS_CACHE = new ClassLoaderWeakCache<Class>();

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

    public static <T> T newInstance(ClassLoader classLoader, final String className) throws Exception {
        classLoader = classLoader == null ? ClassLoaderUtil.class.getClassLoader() : classLoader;
        Constructor<T> constructor = CONSTRUCTOR_CACHE.get(classLoader, className);
        if (constructor != null) {
            return constructor.newInstance();
        }
        Class<T> klass = (Class<T>) loadClass(classLoader, className);
        return (T) newInstance(klass, classLoader, className);
    }

    public static <T> T newInstance(Class<T> klass, ClassLoader classLoader, String className) throws Exception {
        final Constructor<T> constructor = klass.getDeclaredConstructor();
        if (!constructor.isAccessible()) {
            constructor.setAccessible(true);
        }
        CONSTRUCTOR_CACHE.put(classLoader, className, constructor);
        return constructor.newInstance();
    }

    public static Class<?> loadClass(final ClassLoader classLoader, final String className)
            throws ClassNotFoundException {

        isNotNull(className, "className");
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

        // First try to load it through the given classloader
        if (theClassLoader != null) {
            try {
                return tryLoadClass(className, theClassLoader);
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
            return tryLoadClass(className, theClassLoader);
        }
        return Class.forName(className);
    }

    public static boolean isClassAvailable(final ClassLoader classLoader, final String className) {
        try {
            Class<?> clazz = loadClass(classLoader, className);
            return clazz != null;
        } catch (ClassNotFoundException e) {
            EmptyStatement.ignore(e);
        }
        return false;

    }

    private static Class<?> tryLoadClass(String className, ClassLoader classLoader)
            throws ClassNotFoundException {

        Class<?> clazz;

        if (!CLASS_CACHE_DISABLED) {
            clazz = CLASS_CACHE.get(classLoader, className);
            if (clazz != null) {
                return clazz;
            }
        }

        if (className.startsWith("[")) {
            clazz = Class.forName(className, false, classLoader);
        } else {
            clazz = classLoader.loadClass(className);
        }

        if (!CLASS_CACHE_DISABLED) {
            CLASS_CACHE.put(classLoader, className, clazz);
        }

        return clazz;
    }

    public static boolean isInternalType(Class type) {
        String name = type.getName();
        ClassLoader classLoader = ClassLoaderUtil.class.getClassLoader();
        return type.getClassLoader() == classLoader && name.startsWith(HAZELCAST_BASE_PACKAGE);
    }

    /**
     * Tries to load the given class.
     *
     * @param className Name of the class to load
     * @return Loaded class
     * @throws ClassNotFoundException when the class is not found
     */
    public static Class<?> tryLoadClass(String className) throws ClassNotFoundException {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            return contextClassLoader.loadClass(className);
        }
    }

    /**
     * Indicates whether or not the given class exists
     *
     * @param className Name of the class
     * @return {@code true} if the class exists, {@code false} otherwise
     */
    public static boolean isClassDefined(String className) {
        try {
            tryLoadClass(className);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static final class ClassLoaderWeakCache<V> {
        private final ConcurrentMap<ClassLoader, ConcurrentMap<String, WeakReference<V>>> cache;

        private ClassLoaderWeakCache() {
            // Guess 16 classloaders to not waste to much memory (16 is default concurrency level)
            cache = new ConcurrentReferenceHashMap<ClassLoader, ConcurrentMap<String, WeakReference<V>>>(16);
        }

        private V put(ClassLoader classLoader, String className, V value) {
            ClassLoader cl = classLoader == null ? ClassLoaderUtil.class.getClassLoader() : classLoader;
            ConcurrentMap<String, WeakReference<V>> innerCache = cache.get(cl);
            if (innerCache == null) {
                // Let's guess a start of 100 classes per classloader
                innerCache = new ConcurrentHashMap<String, WeakReference<V>>(100);
                ConcurrentMap<String, WeakReference<V>> old = cache.putIfAbsent(cl, innerCache);
                if (old != null) {
                    innerCache = old;
                }
            }
            innerCache.put(className, new WeakReference<V>(value));
            return value;
        }

        public V get(ClassLoader classloader, String className) {
            isNotNull(className, "className");
            ConcurrentMap<String, WeakReference<V>> innerCache = cache.get(classloader);
            if (innerCache == null) {
                return null;
            }
            WeakReference<V> reference = innerCache.get(className);
            V value = reference == null ? null : reference.get();
            if (reference != null && value == null) {
                innerCache.remove(className);
            }
            return value;
        }
    }
}
