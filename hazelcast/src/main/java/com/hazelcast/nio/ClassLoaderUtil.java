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

package com.hazelcast.nio;

import com.hazelcast.internal.usercodedeployment.impl.ClassSource;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.util.ConcurrentReferenceHashMap;
import com.hazelcast.util.ExceptionUtil;

import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.Preconditions.isNotNull;
import static java.util.Collections.unmodifiableMap;

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
        PRIMITIVE_CLASSES = unmodifiableMap(primitives);
    }

    private ClassLoaderUtil() {
    }

    /**
     * Returns the {@code instance} if not null, otherwise constructs a new instance of the class using
     * {@link #newInstance(Class, ClassLoader, String)}.
     *
     * @param instance    the instance of the class, can be null
     * @param classLoader the classloader used for class instantiation
     * @param className   the name of the class being constructed
     * @return either the provided {@code instance} or a newly constructed instance of {@code className}
     */
    public static <T> T getOrCreate(T instance, ClassLoader classLoader, String className) {
        if (instance != null) {
            return instance;
        } else if (className != null) {
            try {
                return ClassLoaderUtil.newInstance(classLoader, className);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T newInstance(ClassLoader classLoader, final String className) throws Exception {
        classLoader = classLoader == null ? ClassLoaderUtil.class.getClassLoader() : classLoader;
        Constructor<T> constructor = CONSTRUCTOR_CACHE.get(classLoader, className);
        if (constructor != null) {
            return constructor.newInstance();
        }
        Class<T> klass = (Class<T>) loadClass(classLoader, className);
        return newInstance(klass, classLoader, className);
    }

    public static <T> T newInstance(Class<T> klass, ClassLoader classLoader, String className) throws Exception {
        final Constructor<T> constructor = klass.getDeclaredConstructor();
        if (!constructor.isAccessible()) {
            constructor.setAccessible(true);
        }
        if (!shouldBypassCache(klass)) {
            CONSTRUCTOR_CACHE.put(classLoader, className, constructor);
        }
        return constructor.newInstance();
    }

    public static Class<?> loadClass(final ClassLoader classLoader, final String className) throws ClassNotFoundException {
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

        // first try to load it through the given classloader
        if (theClassLoader != null) {
            try {
                return tryLoadClass(className, theClassLoader);
            } catch (ClassNotFoundException ignore) {
                // reset selected classloader and try with others
                theClassLoader = null;
            }
        }

        // if failed and this is a Hazelcast class try again with our classloader
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
            return false;
        }
    }

    private static Class<?> tryLoadClass(String className, ClassLoader classLoader) throws ClassNotFoundException {
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
            if (!shouldBypassCache(clazz)) {
                CLASS_CACHE.put(classLoader, className, clazz);
            }
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

    /**
     * Check whether given class implements an interface with the same name.
     * It returns true even when the implemented interface is loaded by a different
     * classloader and hence the class is not assignable into it.
     *
     * An interface is considered as implemented even when either:
     * <ul>
     *     <li>The class directly implements the interface</li>
     *     <li>The class implements an interface which extends the original interface<li></li>
     *     <li>One of superclasses directly implements the interface</li>
     *     <li>One of superclasses implements an interface which extends the original interface</li>
     * </ul>
     *
     * This is useful for logging purposes.
     *
     * @param clazz class to check whether implements the interface
     * @param iface interface to be implemented
     * @return <code>true</code> when the class implements the inteface with the same name
     */
    public static boolean implementsInterfaceWithSameName(Class<?> clazz, Class<?> iface) {
        Class<?>[] interfaces = getAllInterfaces(clazz);
        for (Class implementedInterface : interfaces) {
            if (implementedInterface.getName().equals(iface.getName())) {
                return true;
            }
        }
        return false;
    }

    public static Class<?>[] getAllInterfaces(Class<?> clazz) {
        Collection<Class<?>> interfaces = new HashSet<Class<?>>();
        addOwnInterfaces(clazz, interfaces);
        addInterfacesOfSuperclasses(clazz, interfaces);
        return interfaces.toArray(new Class<?>[0]);
    }

    private static void addOwnInterfaces(Class<?> clazz, Collection<Class<?>> allInterfaces) {
        Class<?>[] interfaces = clazz.getInterfaces();
        Collections.addAll(allInterfaces, interfaces);
        for (Class cl : interfaces) {
            addOwnInterfaces(cl, allInterfaces);
        }
    }

    private static void addInterfacesOfSuperclasses(Class<?> clazz, Collection<Class<?>> interfaces) {
        Class<?> superClass = clazz.getSuperclass();
        while (superClass != null) {
            addOwnInterfaces(superClass, interfaces);
            superClass = superClass.getSuperclass();
        }
    }

    private static final class ClassLoaderWeakCache<V> {

        private final ConcurrentMap<ClassLoader, ConcurrentMap<String, WeakReference<V>>> cache;

        private ClassLoaderWeakCache() {
            // let's guess 16 classloaders to not waste too much memory (16 is default concurrency level)
            cache = new ConcurrentReferenceHashMap<ClassLoader, ConcurrentMap<String, WeakReference<V>>>(16);
        }

        private void put(ClassLoader classLoader, String className, V value) {
            ClassLoader cl = classLoader == null ? ClassLoaderUtil.class.getClassLoader() : classLoader;
            ConcurrentMap<String, WeakReference<V>> innerCache = cache.get(cl);
            if (innerCache == null) {
                // let's guess a start of 100 classes per classloader
                innerCache = new ConcurrentHashMap<String, WeakReference<V>>(100);
                ConcurrentMap<String, WeakReference<V>> old = cache.putIfAbsent(cl, innerCache);
                if (old != null) {
                    innerCache = old;
                }
            }
            innerCache.put(className, new WeakReference<V>(value));
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

    private static boolean shouldBypassCache(Class clazz) {
        // dynamically loaded class should not be cached here, as they are already
        // cached in the DistributedLoadingService (when cache is enabled)
        return (clazz.getClassLoader() instanceof ClassSource);
    }
}
