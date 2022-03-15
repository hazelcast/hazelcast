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

import com.hazelcast.internal.util.JavaVersion.FutureJavaVersion;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JavaVersionTest extends HazelcastTestSupport {

    @Test
    public void testIsAtLeast() {
        JavaVersion[] javaVersions = JavaVersion.values();
        for (JavaVersion thisVersion : javaVersions) {
            for (JavaVersion thatVersion : javaVersions) {
                boolean expected = thisVersion.getMajorVersion() >= thatVersion.getMajorVersion();
                boolean actual = JavaVersion.isAtLeast(thisVersion, thatVersion);
                assertEquals(thisVersion.name() + " should be at least " + thatVersion.name(), expected, actual);
            }

            // we test each versions against not listed versions with the same and the subsequent version number
            assertTrue(JavaVersion.isAtLeast(thisVersion, new FutureJavaVersion(thisVersion.getMajorVersion())));
            assertFalse(JavaVersion.isAtLeast(thisVersion, new FutureJavaVersion(thisVersion.getMajorVersion() + 1)));
        }
    }

    @Test
    public void testIsAtMost() {
        JavaVersion[] javaVersions = JavaVersion.values();
        for (JavaVersion thisVersion : javaVersions) {
            for (JavaVersion thatVersion : javaVersions) {
                boolean expected = thisVersion.getMajorVersion() <= thatVersion.getMajorVersion();
                boolean actual = JavaVersion.isAtMost(thisVersion, thatVersion);
                assertEquals(thisVersion.name() + " should be at most " + thatVersion.name(), expected, actual);
            }

            // we test each versions against not listed versions with the previous and the same version number
            assertFalse(JavaVersion.isAtMost(thisVersion, new FutureJavaVersion(thisVersion.getMajorVersion() - 1)));
            assertTrue(JavaVersion.isAtMost(thisVersion, new FutureJavaVersion(thisVersion.getMajorVersion())));
        }
    }
}
