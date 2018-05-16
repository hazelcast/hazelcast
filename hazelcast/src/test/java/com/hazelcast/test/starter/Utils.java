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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.Assert.assertEquals;

public class Utils {
    public static final boolean DEBUG_ENABLED = false;

    public static RuntimeException rethrow(Exception e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        throw new GuardianException(e);
    }

    /**
     * Transfers the given throwable to the class loader hosting the
     * compatibility tests.
     *
     * @param throwable the throwable to transfer.
     * @return the transferred throwable.
     */
    public static Throwable transferThrowable(Throwable throwable) {
        if (throwable.getClass().getClassLoader() == Utils.class.getClassLoader()) {
            return throwable;
        }

        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(throwable);
            objectOutputStream.close();
            byteArrayOutputStream.close();
            byte[] serializedThrowable = byteArrayOutputStream.toByteArray();

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serializedThrowable);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            Throwable transferredThrowable = (Throwable) objectInputStream.readObject();
            objectInputStream.close();
            byteArrayInputStream.close();

            return transferredThrowable;
        } catch (Exception e) {
            throw new GuardianException("Throwable transfer failed for: " + throwable, e);
        }
    }

    public static void debug(String text) {
        if (DEBUG_ENABLED) {
            System.out.println(text);
        }
    }

    // When running compatibility tests, Hazelcast classes are loaded by various ClassLoaders, so instanceof
    // conditions fail even though it's the same class loaded on a different classloader. In this case, it is
    // desirable to assert an object is an instance of a class by its name
    public static void assertInstanceOfByClassName(String className, Object object) {
        assertEquals(className, object.getClass().getName());
    }
}
