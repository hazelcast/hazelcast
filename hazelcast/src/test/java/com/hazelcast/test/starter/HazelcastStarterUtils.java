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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static com.hazelcast.nio.IOUtil.closeResource;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("WeakerAccess")
public class HazelcastStarterUtils {

    private static final boolean DEBUG_ENABLED = false;

    private static final ILogger LOGGER = Logger.getLogger(HazelcastStarterUtils.class);

    public static RuntimeException rethrowGuardianException(Exception e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        throw new GuardianException(e);
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
     * Transfers the given throwable to the class loader hosting the
     * compatibility tests.
     *
     * @param throwable the throwable to transfer
     * @return the transferred throwable
     */
    public static Throwable transferThrowable(Throwable throwable) {
        if (throwable.getClass().getClassLoader() == HazelcastStarterUtils.class.getClassLoader()) {
            return throwable;
        }

        ByteArrayOutputStream byteArrayOutputStream = null;
        ObjectOutputStream objectOutputStream = null;
        ByteArrayInputStream byteArrayInputStream = null;
        ObjectInputStream objectInputStream = null;
        try {
            byteArrayOutputStream = new ByteArrayOutputStream();
            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(throwable);
            byte[] serializedThrowable = byteArrayOutputStream.toByteArray();

            byteArrayInputStream = new ByteArrayInputStream(serializedThrowable);
            objectInputStream = new ObjectInputStream(byteArrayInputStream);
            return (Throwable) objectInputStream.readObject();
        } catch (Exception e) {
            throw new GuardianException("Throwable transfer failed for: " + throwable, e);
        } finally {
            closeResource(objectInputStream);
            closeResource(byteArrayInputStream);
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
}
