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

package com.hazelcast.test.starter;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("WeakerAccess")
public class HazelcastStarterUtils {

    private static final boolean DEBUG_ENABLED = false;

    private static final ILogger LOGGER = Logger.getLogger(HazelcastStarterUtils.class);

    public static RuntimeException rethrowGuardianException(Throwable t) {
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        }
        throw new GuardianException(t);
    }

    public static boolean isDebugEnabled() {
        return DEBUG_ENABLED;
    }

    public static void debug(String text) {
        if (DEBUG_ENABLED) {
            LOGGER.info(text);
        }
    }

    public static void debug(String text, Object... args) {
        if (DEBUG_ENABLED) {
            LOGGER.info(format(text, args));
        }
    }

    /**
     * Transfers the given {@link Throwable} to the classloader hosting the
     * compatibility tests.
     *
     * @param throwable the Throwable to transfer
     * @return the transferred Throwable
     */
    public static Throwable transferThrowable(Throwable throwable) {
        return transferToCurrentClassloader(throwable);
    }

    /**
     * Transfers the given object to the classloader hosting the compatibility
     * tests.
     *
     * @param object the object to transfer
     * @param <T>    the type of the object
     * @return the transferred object
     */
    public static <T> T transferToCurrentClassloader(T object) {
        if (object.getClass().getClassLoader() == HazelcastStarterUtils.class.getClassLoader()) {
            return object;
        }

        ByteArrayOutputStream byteArrayOutputStream = null;
        ObjectOutputStream objectOutputStream = null;
        ByteArrayInputStream byteArrayInputStream = null;
        ObjectInputStream objectInputStream = null;
        try {
            byteArrayOutputStream = new ByteArrayOutputStream();
            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(object);
            byte[] serializedObject = byteArrayOutputStream.toByteArray();

            byteArrayInputStream = new ByteArrayInputStream(serializedObject);
            objectInputStream = new ObjectInputStream(byteArrayInputStream);
            //noinspection unchecked
            return (T) objectInputStream.readObject();
        } catch (Exception e) {
            throw new GuardianException("Object transfer via serialization failed for: " + object, e);
        } finally {
            closeResource(objectInputStream);
            closeResource(byteArrayInputStream);
            closeResource(objectOutputStream);
            closeResource(byteArrayOutputStream);
        }
    }

    /**
     * Transfers the given object to the given target classloader.
     *
     * @param object            the object to transfer
     * @param targetClassloader the given target classloader
     * @param <T>               the type of the object
     * @return the transferred object
     */
    public static <T> T transferToClassloader(T object, ClassLoader targetClassloader) throws Exception {
        if (object.getClass().getClassLoader() == targetClassloader) {
            return object;
        }

        Class<?> byteArrayInputStreamClass = targetClassloader.loadClass(ByteArrayInputStream.class.getName());
        Class<?> objectInputStreamClass = targetClassloader.loadClass(ObjectInputStream.class.getName());
        Class<?> inputStreamClass = targetClassloader.loadClass(InputStream.class.getName());
        Constructor<?> byteArrayInputStreamConstructor = byteArrayInputStreamClass.getConstructor(byte[].class);
        Constructor<?> objectInputStreamConstructor = objectInputStreamClass.getConstructor(inputStreamClass);
        Method byteArrayInputStreamCloseMethod = byteArrayInputStreamClass.getMethod("close");
        Method objectInputStreamCloseMethod = objectInputStreamClass.getMethod("close");
        Method objectInputStreamReadObjectMethod = objectInputStreamClass.getMethod("readObject");

        ByteArrayOutputStream byteArrayOutputStream = null;
        ObjectOutputStream objectOutputStream = null;
        Object byteArrayInputStream = null;
        Object objectInputStream = null;
        try {
            byteArrayOutputStream = new ByteArrayOutputStream();
            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(object);
            byte[] serializedObject = byteArrayOutputStream.toByteArray();

            byteArrayInputStream = byteArrayInputStreamConstructor.newInstance(new Object[]{serializedObject});
            objectInputStream = objectInputStreamConstructor.newInstance(byteArrayInputStream);
            //noinspection unchecked
            return (T) objectInputStreamReadObjectMethod.invoke(objectInputStream);
        } catch (Exception e) {
            throw new GuardianException("Object transfer via serialization failed for: " + object, e);
        } finally {
            if (objectInputStream != null) {
                objectInputStreamCloseMethod.invoke(objectInputStream);
            }
            if (byteArrayInputStream != null) {
                byteArrayInputStreamCloseMethod.invoke(byteArrayInputStream);
            }
            closeResource(objectOutputStream);
            closeResource(byteArrayOutputStream);
        }
    }

    /**
     * Asserts the instanceOf() by the classname only.
     * <p>
     * When running compatibility tests, Hazelcast classes are loaded by
     * various classloaders, so instanceof conditions fail even though it's
     * the same class loaded on a different classloader. In this case, it's
     * desirable to assert an object is an instance of a class by its name.
     *
     * @param className the expected classname (FQCN, not simple classname)
     * @param object    the instance to check
     */
    public static void assertInstanceOfByClassName(String className, Object object) {
        assertEquals(className, object.getClass().getName());
    }

    /**
     * Returns a {@link Collection} object for a given collection interface.
     *
     * @return a new Collection object of a class that is assignable from the given type
     * @throws UnsupportedOperationException if the given interface is not implemented
     */
    public static Collection<Object> newCollectionFor(Class<?> type) {
        if (Set.class.isAssignableFrom(type)) {
            // original set might be ordered
            return new LinkedHashSet<Object>();
        } else if (List.class.isAssignableFrom(type)) {
            return new ArrayList<Object>();
        } else if (Queue.class.isAssignableFrom(type)) {
            return new ConcurrentLinkedQueue<Object>();
        } else if (Collection.class.isAssignableFrom(type)) {
            return new LinkedList<Object>();
        } else {
            throw new UnsupportedOperationException("Cannot locate collection type for " + type);
        }
    }
}
