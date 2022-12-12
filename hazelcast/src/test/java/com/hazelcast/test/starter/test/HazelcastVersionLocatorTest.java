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

package com.hazelcast.test.starter.test;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.starter.HazelcastVersionLocator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Map;

import static com.google.common.io.Files.toByteArray;
import static com.hazelcast.test.starter.HazelcastVersionLocator.Artifact.EE_JAR;
import static com.hazelcast.test.starter.HazelcastVersionLocator.Artifact.EE_TEST_JAR;
import static com.hazelcast.test.starter.HazelcastVersionLocator.Artifact.OS_JAR;
import static com.hazelcast.test.starter.HazelcastVersionLocator.Artifact.OS_TEST_JAR;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("UnstableApiUsage")
@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class HazelcastVersionLocatorTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @SuppressWarnings("deprecation")
    private final HashFunction md5Hash = Hashing.md5();

    @Test
    public void testDownloadVersion() throws Exception {
        Map<HazelcastVersionLocator.Artifact, File> files = HazelcastVersionLocator.locateVersion("4.0", true);

        assertHash(files.get(OS_JAR), "bc409b12b96ece6d05c3bd1e99b202bb", "OS");

        assertHash(files.get(OS_TEST_JAR), "220509ece9fc152525c91ba7c75ce600", "OS tests");

        assertHash(files.get(EE_JAR), "765816e628ca4ca57d5bd7387e761eaa", "EE");

        assertHash(files.get(EE_TEST_JAR), "162bcb2412570845e6fd91ee61b54f94", "EE tests");
    }

    private void assertHash(File file, String expectedHash, String label) throws Exception {
        byte[] memberBytes = toByteArray(file);
        HashCode memberHash = md5Hash.hashBytes(memberBytes);
        assertEquals("Expected hash of Hazelcast " + label + " JAR to be " + expectedHash, expectedHash, memberHash.toString());
    }
}
