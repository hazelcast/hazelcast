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

package com.hazelcast.internal.util;

import com.hazelcast.internal.util.ModularJavaUtils.PackageAccessRequirement;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.util.ModularJavaUtils.PackageAccessRequirement.open;
import static com.hazelcast.internal.util.ModularJavaUtils.PackageAccessRequirement.packages;
import static com.hazelcast.internal.util.ModularJavaUtils.checkJavaInternalAccess;
import static com.hazelcast.internal.util.ModularJavaUtils.checkPackageRequirements;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests that {@link ModularJavaUtils} correctly logs warnings about missing package access on Java 9+.
 */
@Category({ QuickTest.class, ParallelJVMTest.class })
public class ModularJavaUtilsTest {

    /**
     * Tests that the testsuite is running without missing-package-access warnings. I.e. the access to Java internal packages is
     * provided.
     */
    @Test
    public void testNoMissingPackageAccessInTestsuite() {
        ILogger logger = mock(ILogger.class);

        checkJavaInternalAccess(logger);

        verify(logger, never()).warning(anyString());
    }

    /**
     * Tests that {@link ModularJavaUtils#checkPackageRequirements(ILogger, Map)} logs a warning with the missing
     * Java argument if a Java internal package access not provided to Hazelcast and the test is running on Java 9 or newer.
     */
    @Test
    public void testMissingAccess() {
        assumeTrue(JavaVersion.isAtLeast(JavaVersion.JAVA_9));

        ILogger logger = mock(ILogger.class);

        Map<String, PackageAccessRequirement[]> requirements = new HashMap<>();
        requirements.put("java.base", packages(open("jdk.internal.misc")));
        checkPackageRequirements(logger, requirements);

        verify(logger, times(1)).warning(contains("--add-opens java.base/jdk.internal.misc=ALL-UNNAMED"));
    }

    @Test
    public void testHazelcastModuleName() {
        assertNull("Hazelcast module name should be null as the testsuite runs hazelcast on the classpath",
                ModularJavaUtils.getHazelcastModuleName());
    }

    @Test
    public void testNoExceptionWhenNullLogger() {
        checkJavaInternalAccess(null);
    }

}
