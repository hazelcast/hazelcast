/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl.executejar;

import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MainMethodFinderTest {

    // Empty method for testing testPublicAndStatic
    public static void main(String[] args) {
    }

    @Test
    public void testPublicAndStaticForMain() throws NoSuchMethodException {
        Method method = MainMethodFinderTest.class.getDeclaredMethod("main", String[].class);

        MainMethodFinder mainMethodFinder = new MainMethodFinder();
        mainMethodFinder.mainMethod = method;
        boolean publicAndStatic = mainMethodFinder.isPublicAndStatic();
        assertTrue(publicAndStatic);
    }

    @Test
    public void testPublicAndStaticForSelf() throws NoSuchMethodException {
        Method method = MainMethodFinderTest.class.getDeclaredMethod("testPublicAndStaticForSelf");

        MainMethodFinder mainMethodFinder = new MainMethodFinder();
        mainMethodFinder.mainMethod = method;
        boolean publicAndStatic = mainMethodFinder.isPublicAndStatic();
        assertFalse(publicAndStatic);
    }

    @Test
    public void testGetMainMethod() {
        MainMethodFinder mainMethodFinder = new MainMethodFinder();
        mainMethodFinder.getMainMethodOfClass(MainMethodFinderTest.class);
        assertNotNull(mainMethodFinder.mainMethod);
    }
}
