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

package com.hazelcast.internal.nio;

import com.hazelcast.internal.usercodedeployment.impl.ClassSource;
import com.hazelcast.internal.util.ConcurrentReferenceHashMap;
import com.hazelcast.internal.util.ExceptionUtil;

import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.Preconditions.isNotNull;
import static java.util.Collections.unmodifiableMap;

/**
 * Utility class to deal with class loaders.
 */
@SuppressWarnings({"checkstyle:magicnumber", "checkstyle:npathcomplexity"})
public final class ClassLoaderUtil {

    public static final String HAZELCAST_BASE_PACKAGE = "com.hazelcast.";
    public static final String HAZELCAST_ARRAY = "[L" + HAZELCAST_BASE_PACKAGE;

    private static final boolean CLASS_CACHE_DISABLED = Boolean.getBoolean("hazelcast.compat.classloading.cache.disabled");

    private static final Map<String, Class> PRIMITIVE_CLASSES;
    private static final int MAX_PRIM_CLASS_NAME_LENGTH = 7;

    private static final ClassLoaderWeakCache<Constructor> CONSTRUCTOR_CACHE = new ClassLoaderWeakCache<Constructor>();
    private static final ClassLoaderWeakCache<Class> CLASS_CACHE = new ClassLoaderWeakCache<Class>();
    private static final Constructor<?> IRRESOLVABLE_CONSTRUCTOR;

    private static final ClassLoader NULL_FALLBACK_CLASSLOADER = new URLClassLoader(new URL[0],
            ClassLoaderUtil.class.getClassLoader());

    static {
        try {
            IRRESOLVABLE_CONSTRUCTOR = IrresolvableConstructor.class.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new Error("Couldn't initialize irresolvable constructor.", e);
        }

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
     * Returns the {@code instance}, if not null, or constructs a new instance of the class using
     * {@link #newInstance(ClassLoader, String)}.
     *
     * @param instance    the instance of the class, can be null
     * @param classLoader the classloader used for class instantiation
     * @param className   the name of the class being constructed. If null, null is returned.
     * @return the provided {@code instance} or a newly constructed instance of {@code className}
     * or null, if {@code className} was null
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

    /**
     * Creates a new instance of class with {@code className}, using the no-arg
     * constructor. Preferably uses the class loader specified in {@code
     * classLoaderHint}. A constructor cache is used to reduce reflection
     * calls.
     *
     * <p>The implementation first chooses candidate class loaders. Then checks
     * the constructor cache if a constructor is found for either of them. If
     * not, it queries them and caches the used {@code Constructor} under the
     * classloader key that was used to retrieve it (might not the actual
     * classloader that loaded the returned instance, but a parent one). To
     * choose the candidate class loaders, a peculiar,
     * hard-to-explain-more-simply-than-reading-the-code logic is used, beware.
     *
     * @param classLoaderHint Suggested class loader to use, can be null
     * @param className       Class name (can be also primitive name or array
     *                        ("[Lpkg.Class]"), required
     * @return New instance
     * @throws Exception ClassNotFoundException, IllegalAccessException,
     *                   InstantiationException, or InvocationTargetException
     */
    @SuppressWarnings({"unchecked", "checkstyle:cyclomaticcomplexity"})
    public static <T> T newInstance(final ClassLoader classLoaderHint, final String className) throws Exception {
        isNotNull(className, "className");
        final Class primitiveClass = tryPrimitiveClass(className);
        if (primitiveClass != null) {
            // Note: this will always throw java.lang.InstantiationException
            return (T) primitiveClass.newInstance();
        }

        // Note: Class.getClassLoader() and Thread.getContextClassLoader() are
        // allowed to return null

        // Note2: If classLoaderHint is not-null and the class' package is
        // HAZELCAST_BASE_PACKAGE, the thread's context class loader won't be
        // checked. We assume that if a classLoaderHint is given, the caller
        // knows the class should be found in that particular class loader. We
        // can't assume that all classes in `com.hazelcast` package are part of
        // this project, they can be samples, other HZ projects such as Jet etc.

        ClassLoader cl1 = classLoaderHint;
        if (cl1 == null) {
            cl1 = ClassLoaderUtil.class.getClassLoader();
        }
        if (cl1 == null) {
            cl1 = Thread.currentThread().getContextClassLoader();
        }
        ClassLoader cl2 = null;
        if ((className.startsWith(HAZELCAST_BASE_PACKAGE) || className.startsWith(HAZELCAST_ARRAY))
                && cl1 != ClassLoaderUtil.class.getClassLoader()) {
            cl2 = ClassLoaderUtil.class.getClassLoader();
        }
        if (cl2 == null) {
            cl2 = Thread.currentThread().getContextClassLoader();
        }
        if (cl1 == cl2) {
            cl2 = null;
        }
        if (cl1 == null && cl2 != null) {
            cl1 = cl2;
            cl2 = null;
        }

        // check candidate class loaders in cache
        // note that cl1 might be null at this point: we'll use null key. In that case
        // we have no class loader to use
        if (cl1 != null) {
            Constructor<T> constructor = CONSTRUCTOR_CACHE.get(cl1, className);

            // If a constructor in question is not found in the preferred/hinted
            // class loader (cl1) constructor cache, that doesn't mean it can't
            // be provided by cl1. So we try to create an object instance using
            // cl1 first if its constructor is not marked as irresolvable in cl1
            // cache.
            //
            // This is important when both class loaders provide a class named
            // exactly the same. Such situations are not prohibited by JVM and
            // we have to resolve "conflicts" somehow, we prefer cl1 over cl2.

            if (constructor == IRRESOLVABLE_CONSTRUCTOR && cl2 != null) {
                constructor = CONSTRUCTOR_CACHE.get(cl2, className);
            }

            if (constructor != null && constructor != IRRESOLVABLE_CONSTRUCTOR) {
                return constructor.newInstance();
            }
        }

        // if not found in cache, try to query the class loaders and add constructor to cache
        try {
            return newInstance0(cl1, className);
        } catch (ClassNotFoundException e1) {
            if (cl2 != null) {
                // Mark as irresolvable only if we were trying to give it a
                // priority over cl2 to save cl1 cache space.
                CONSTRUCTOR_CACHE.put(cl1, className, IRRESOLVABLE_CONSTRUCTOR);

                try {
                    return newInstance0(cl2, className);
                } catch (ClassNotFoundException e2) {
                    ignore(e2);
                }
            }
            throw e1;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T newInstance0(ClassLoader classLoader, String className) throws Exception {
        Class klass = classLoader == null ? Class.forName(className) : tryLoadClass(className, classLoader);
        return newInstance(classLoader, klass);
    }

    @SuppressWarnings("unchecked")
    public static <T> T newInstance(ClassLoader classLoader, Class klass) throws Exception {
        final Constructor constructor = klass.getDeclaredConstructor();
        if (!constructor.isAccessible()) {
            constructor.setAccessible(true);
        }
        if (!shouldBypassCache(klass) && classLoader != null) {
            CONSTRUCTOR_CACHE.put(classLoader, klass.getName(), constructor);
        }
        return (T) constructor.newInstance();
    }

    public static Class<?> loadClass(final ClassLoader classLoaderHint, final String className) throws ClassNotFoundException {
        isNotNull(className, "className");
        final Class<?> primitiveClass = tryPrimitiveClass(className);
        if (primitiveClass != null) {
            return primitiveClass;
        }

        // Try to load it using the hinted classloader if not null
        if (classLoaderHint != null) {
            try {
                return tryLoadClass(className, classLoaderHint);
            } catch (ClassNotFoundException ignore) {
            }
        }

        // If this is a Hazelcast class, try to load it using our classloader
        ClassLoader theClassLoader = ClassLoaderUtil.class.getClassLoader();
        if (theClassLoader != null && belongsToHazelcastPackage(className)) {
            try {
                return tryLoadClass(className, ClassLoaderUtil.class.getClassLoader());
            } catch (ClassNotFoundException ignore) {
            }
        }

        // Try to load it using context class loader if not null
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader != null) {
            return tryLoadClass(className, contextClassLoader);
        }
        return tryLoadClass(className, NULL_FALLBACK_CLASSLOADER);
    }

    private static boolean belongsToHazelcastPackage(String className) {
        return className.startsWith(HAZELCAST_BASE_PACKAGE) || className.startsWith(HAZELCAST_ARRAY);
    }

    private static Class<?> tryPrimitiveClass(String className) {
        if (className.length() <= MAX_PRIM_CLASS_NAME_LENGTH && Character.isLowerCase(className.charAt(0))) {
            final Class primitiveClass = PRIMITIVE_CLASSES.get(className);
            if (primitiveClass != null) {
                return primitiveClass;
            }
        }
        return null;
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

        if (classLoader == NULL_FALLBACK_CLASSLOADER) {
            clazz = Class.forName(className);
        } else if (className.startsWith("[")) {
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
     * <p>
     * An interface is considered as implemented when either:
     * <ul>
     *     <li>The class directly implements the interface</li>
     *     <li>The class implements an interface which extends the original interface</li>
     *     <li>One of superclasses directly implements the interface</li>
     *     <li>One of superclasses implements an interface which extends the original interface</li>
     * </ul>
     * <p>
     * This is useful for logging purposes.
     *
     * @param clazz class to check whether implements the interface
     * @param iface interface to be implemented
     * @return <code>true</code> when the class implements the interface with the same name
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
            // let's guess 16 class loaders to not waste too much memory (16 is default concurrency level)
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

    private static final class IrresolvableConstructor {
        /**
         * Works as a marker for irresolvable constructors.
         */
        @SuppressWarnings("checkstyle:RedundantModifier")
        public IrresolvableConstructor() {
            throw new UnsupportedOperationException("Irresolvable constructor must never be instantiated.");
        }
    }

}
