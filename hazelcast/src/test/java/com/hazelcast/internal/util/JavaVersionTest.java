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

package com.hazelcast.internal.util;

import com.hazelcast.internal.util.JavaVersion.FutureJavaVersion;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.internal.util.JavaVersion.UNKNOWN_VERSION;
import static com.hazelcast.test.OverridePropertyRule.clear;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class JavaVersionTest extends HazelcastTestSupport {
    @Rule
    public final OverridePropertyRule overrideJavaVersion = clear("java.version");

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
    public void testVersionDetection() {
        // java.version is empty
        assertEquals(UNKNOWN_VERSION, JavaVersion.detectCurrentVersion());

        overrideJavaVersion.setOrClearProperty("ea-99");
        assertEquals(UNKNOWN_VERSION, JavaVersion.detectCurrentVersion());

        overrideJavaVersion.setOrClearProperty("99-ea");
        assertThat(JavaVersion.detectCurrentVersion())
                .isInstanceOf(FutureJavaVersion.class)
                .matches(v -> v.getMajorVersion() == 99);
    }

    @Test
    public void testUnknownVersion() {
        List<JavaMajorVersion> versions = new ArrayList<>(Arrays.<JavaMajorVersion>asList(JavaVersion.values()));
        versions.add(new FutureJavaVersion(99));
        for (JavaMajorVersion version : versions) {
            assertFalse(JavaVersion.isAtLeast(UNKNOWN_VERSION, version));
            assertFalse(JavaVersion.isAtLeast(version, UNKNOWN_VERSION));

        }
        assertFalse(JavaVersion.isAtLeast(UNKNOWN_VERSION, UNKNOWN_VERSION));
    }
}
