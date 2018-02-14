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

import static org.junit.Assert.assertEquals;

public class Utils {
    public static final boolean DEBUG_ENABLED = false;

    public static RuntimeException rethrow(Exception e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        throw new GuardianException(e);
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
