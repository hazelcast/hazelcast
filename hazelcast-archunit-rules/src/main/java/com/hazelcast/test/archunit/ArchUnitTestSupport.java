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

package com.hazelcast.test.archunit;

import org.hamcrest.Matchers;
import org.junit.Assume;
import org.junit.BeforeClass;

import java.lang.reflect.Method;

public abstract class ArchUnitTestSupport {

    private static final int HIGHEST_JDK = 21;

    // ArchUnit releases lag behind the JDK releases.
    // Skip the test if JDK version is higher than the specified assumption
    @BeforeClass
    public static void beforeClass() throws Throwable {
        Assume.assumeThat("Skipping as ASM shaded within ArchUnit 1.0.1 doesn't support Java 21", getCurrentJavaVersion(),
                Matchers.is(Matchers.lessThan(HIGHEST_JDK)));
    }

    private static int getCurrentJavaVersion() throws Throwable {
        Class runtimeClass = Runtime.class;

        Class versionClass = Class.forName("java.lang.Runtime$Version");
        Method versionMethod = runtimeClass.getDeclaredMethod("version");
        Object versionObj = versionMethod.invoke(Runtime.getRuntime());
        Method majorMethod = versionClass.getDeclaredMethod("major");
        return (int) majorMethod.invoke(versionObj);

    }
}
