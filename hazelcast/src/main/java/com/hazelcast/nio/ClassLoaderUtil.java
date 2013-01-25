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

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @mdogan 4/12/12
 */
public final class ClassLoaderUtil {

    public static final String HAZELCAST_BASE_PACKAGE = "com.hazelcast.";
    public static final String HAZELCAST_ARRAY = "[L" + HAZELCAST_BASE_PACKAGE;

    private static final Map<String, Class> PRIMITIVE_CLASSES;
    private static final int MAX_PRIM_CLASSNAME_LENGTH = 7; // boolean.class.getName().length();

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

    public static <T> T newInstance(final String className) throws Exception {
        return (T) newInstance(loadClass(className));
    }

    public static <T> T newInstance(final Class<T> klass) throws Exception {
        final Constructor<T> constructor = klass.getDeclaredConstructor();
        if (!constructor.isAccessible()) {
            constructor.setAccessible(true);
        }
        return constructor.newInstance();
    }

    public static Class<?> loadClass(final String className) throws ClassNotFoundException {
        return loadClass(null, className);
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
                return Class.forName(className, true, theClassLoader);
            } else {
                return theClassLoader.loadClass(className);
            }
        }
        return Class.forName(className);
    }

    public static boolean isInternalType(Class type) {
        return type.getClassLoader() == ClassLoaderUtil.class.getClassLoader()
            && type.getName().startsWith(HAZELCAST_BASE_PACKAGE);
    }

    private ClassLoaderUtil() {}
}
