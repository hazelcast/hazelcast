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

import java.io.Serializable;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.reflect.Constructor;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * @author mdogan 4/12/12
 */
public final class ClassLoaderUtil {

    public static final String HAZELCAST_BASE_PACKAGE = "com.hazelcast.";
    public static final String HAZELCAST_ARRAY = "[L" + HAZELCAST_BASE_PACKAGE;

    private static final Map<String, Class> PRIMITIVE_CLASSES;
    private static final int MAX_PRIM_CLASSNAME_LENGTH = 7; // boolean.class.getName().length();

    private static final ILogger logger = Logger.getLogger(ClassLoaderUtil.class);

    /**
     * Class cache. Using soft references to not hinder GC.
     * No upper limits, but should not be a problem,
     * since we only add some {@link SoftReference} objects to the already existing memory usage.
     */
    private static final Map<ClassCachePK, Class<?>> classCache = new SoftConcurrentHashMap<ClassCachePK, Class<?>>();

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

    public static <T> T newInstance(ClassLoader classLoader, final String className) throws Exception {
        return (T) newInstance(loadClass(classLoader, className));
    }

    public static <T> T newInstance(final Class<T> klass) throws Exception {
        final Constructor<T> constructor = klass.getDeclaredConstructor();
        if (!constructor.isAccessible()) {
            constructor.setAccessible(true);
        }
        return constructor.newInstance();
    }

    public static Class<?> loadClass(final ClassLoader classLoader, final String className)
            throws ClassNotFoundException {
        if (className == null) {
            throw new IllegalArgumentException("ClassName cannot be null!");
        }
        if (className.length() <= MAX_PRIM_CLASSNAME_LENGTH && Character.isLowerCase(className.charAt(0))) {
            final Class primitiveClass = PRIMITIVE_CLASSES.get(className);
            if (primitiveClass != null) {
                return primitiveClass;
            }
        }
        ClassLoader theClassLoader = classLoader;
        if (className.startsWith(HAZELCAST_BASE_PACKAGE) || className.startsWith(HAZELCAST_ARRAY)) {
            theClassLoader = ClassLoaderUtil.class.getClassLoader();
        }
        if (theClassLoader == null) {
            theClassLoader = Thread.currentThread().getContextClassLoader();
        }
        if (theClassLoader != null) {
            if (className.startsWith("[")) {
                return cacheLoad(className, true, theClassLoader);
            } else {
                return cacheLoad(className, false, theClassLoader);
            }
        }
        return cacheLoad(className, true, ClassLoaderUtil.class.getClassLoader());
        //return Class.forName(className);
    }

    public static boolean isInternalType(Class type) {
        return type.getClassLoader() == ClassLoaderUtil.class.getClassLoader()
            && type.getName().startsWith(HAZELCAST_BASE_PACKAGE);
    }

    /**
     * Gets {@link Class} from cache or loads it and and puts it into cache for subsequent look-ups.
     *
     * @param className
     * @param forName whether to use {@link Class#forName(String, boolean, ClassLoader)} (true) or
     *      {@link ClassLoader#loadClass(String)} (false).
     * @param classLoader
     * @return loaded {@link Class}
     * @throws ClassNotFoundException
     */
    private static Class<?> cacheLoad(String className, boolean forName, ClassLoader classLoader)
            throws ClassNotFoundException {
        assert className != null;
        assert classLoader != null;

        final boolean finestEnabled = logger.isFinestEnabled();

        final ClassCachePK classCachePK = new ClassCachePK(className, classLoader);

        Class<?> cachedClass = classCache.get(classCachePK);
        if (cachedClass != null) {
            if (finestEnabled) {
                logger.finest("Found class in cache " + className + ". PK=" + classCachePK);
            }
            return cachedClass;
        }

        // cache miss
        final Class<?> loadedClass;
        if (forName)
        {
            loadedClass = Class.forName(className, true, classLoader);
        }
        else
        {
            loadedClass = classLoader.loadClass(className);
        }

        Class<?> prevousClass = classCache.put(classCachePK, loadedClass);
        if (finestEnabled) {
            if (prevousClass != null) {
                // may also occur if class is added first time due to concurrent calls. But better than synchronizing.
                logger.finest("Replaced cached class " + className + " with new one. Size=" + classCache.size() + ". PK=" + classCachePK);
            }
            else
            {
                logger.finest("Added class to cache " + className + ". Size=" + classCache.size() + ". PK=" + classCachePK);
            }
        }

        return loadedClass;
    }

    private ClassLoaderUtil() {}


    /**
     * ClassCachePK.
     * {@link ClassLoader} does not redefine equals and hashCode, so this may have some potential issues,
     * but usually it works fine and
     * but best way to map a {@link Class} to its class name / {@link ClassLoader} combination.
     *
     * @author Rico Neubauer
     */
    protected static class ClassCachePK {

        private final String className;
        private final ClassLoader classLoader;

        public ClassCachePK(String className, ClassLoader classLoader) {
            this.className = className;
            this.classLoader = classLoader;
        }

        /**
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((classLoader == null) ? 0 : classLoader.hashCode());
            result = prime * result + ((className == null) ? 0 : className.hashCode());
            return result;
        }

        /**
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ClassCachePK other = (ClassCachePK)obj;
            if (classLoader == null) {
                if (other.classLoader != null)
                    return false;
            }
            else if (!classLoader.equals(other.classLoader))
                return false;
            if (className == null) {
                if (other.className != null)
                    return false;
            }
            else if (!className.equals(other.className))
                return false;
            return true;
        }

        /**
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("ClassCachePK [className=");
            builder.append(className);
            builder.append(", classLoader=");
            builder.append(classLoader);
            builder.append("]");
            return builder.toString();
        }

    }


    /**
     * SoftConcurrentHashMap.
     * You can use the SoftConcurrentHashMap just like an ordinary HashMap,
     * except that the entries will disappear if we are running low on memory.
     * Ideal if you want to build a cache.
     *
     * This implementation <b>is</b> thread-safe (just like {@link ConcurrentHashMap}).
     * This class does <b>not</b> allow null to be used as a key or value.
     *
     * Inspired by http://www.javaspecialists.eu/archive/Issue098.html
     *
     * @author Rico Neubauer
     * @since 27.04.2010
     */
    protected static class SoftConcurrentHashMap<K, V> extends AbstractMap<K, V> implements Serializable {
        private static final long serialVersionUID = 1L;

        /** The internal HashMap that will hold the SoftReference. */
        private final Map<K, SoftReference<V>> hash = new ConcurrentHashMap<K, SoftReference<V>>();

        private final Map<SoftReference<V>, K> reverseLookup = new ConcurrentHashMap<SoftReference<V>, K>();

        /** Reference queue for cleared SoftReference objects. */
        protected final ReferenceQueue<V> queue = new ReferenceQueue<V>();


        @Override
        public V get(Object key)
        {
            expungeStaleEntries();
            V result = null;
            // We get the SoftReference represented by that key
            final SoftReference<V> soft_ref = hash.get(key);
            if (soft_ref != null)
            {
                // From the SoftReference we get the value, which can be
                // null if it has been garbage collected
                result = soft_ref.get();
                if (result == null)
                {
                    // If the value has been garbage collected, remove the
                    // entry from the HashMap.
                    hash.remove(key);
                    reverseLookup.remove(soft_ref);
                }
            }
            return result;
        }


        private void expungeStaleEntries()
        {
            Reference<? extends V> sv;
            while ((sv = queue.poll()) != null)
            {
                final K removed = reverseLookup.remove(sv);
                if (removed != null)
                {
                    hash.remove(removed);
                }
            }
        }


        @Override
        public V put(K key, V value)
        {
            expungeStaleEntries();
            final SoftReference<V> soft_ref = new SoftReference<V>(value, queue);
            final SoftReference<V> oldSoftRef = hash.put(key, soft_ref);
            reverseLookup.put(soft_ref, key);
            if (oldSoftRef == null)
            {
                return null;
            }
            reverseLookup.remove(oldSoftRef);
            return oldSoftRef.get();
        }


        @Override
        public V remove(Object key)
        {
            expungeStaleEntries();
            final SoftReference<V> softRef = hash.remove(key);
            // small concurrency gap - reverseLookup still holds entry
            if (softRef == null)
            {
                return null;
            }
            reverseLookup.remove(softRef);
            return softRef.get();
        }


        @Override
        public void clear()
        {
            hash.clear();
            reverseLookup.clear();
        }


        @Override
        public int size()
        {
            expungeStaleEntries();
            return hash.size();
        }


        /**
         * Returns a copy of the key/values in the map at the point of calling.
         * However, setValue still sets the value in the actual SoftHashMap.
         *
         * @return a copy of the key/values in the map at the point of calling.
         * @see java.util.AbstractMap#entrySet()
         * @inheritDoc
         */
        @Override
        public Set<Entry<K, V>> entrySet()
        {
            expungeStaleEntries();
            final Set<Entry<K, V>> result = new LinkedHashSet<Entry<K, V>>();
            for (final Entry<K, SoftReference<V>> entry : hash.entrySet())
            {
                final V value = entry.getValue().get();
                if (value != null)
                {
                    result.add(new Entry<K, V>()
                    {
                        @Override
                        public K getKey()
                        {
                            return entry.getKey();
                        }


                        @Override
                        public V getValue()
                        {
                            return value;
                        }


                        @Override
                        public V setValue(V v)
                        {
                            entry.setValue(new SoftReference<V>(v, queue));
                            return value;
                        }
                    });
                }
            }
            return result;
        }
    }

}
