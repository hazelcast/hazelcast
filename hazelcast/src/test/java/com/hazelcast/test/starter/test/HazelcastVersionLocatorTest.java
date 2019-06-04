/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import static com.google.common.io.Files.toByteArray;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class HazelcastVersionLocatorTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private HashFunction md5Hash = Hashing.md5();

    @Test
    public void testDownloadVersion() throws Exception {
        File[] files = HazelcastVersionLocator.locateVersion("3.6", folder.getRoot(), true);

        assertHash(files[0], "89563f7dab02bd5f592082697c24d167", "OS member");

        assertHash(files[1], "66615c68c2708036a6030114a1b87f2b", "OS member tests");

        assertHash(files[2], "fd6022e35908b42d24fe10a9c9fdaad5", "OS client");

        assertHash(files[3], "c5718ba5c280339fff9b54ecb5e61549", "EE member");

        assertHash(files[4], "c5df750fdab71a650fb56c41742806ff", "EE member tests");

        assertHash(files[5], "b1cf93ec4bb9bcda8809b81349f48cb3", "EE client");
    }

    private void assertHash(File file, String expectedHash, String label) throws Exception {
        byte[] memberBytes = toByteArray(file);
        HashCode memberHash = md5Hash.hashBytes(memberBytes);
        assertEquals("Expected hash of Hazelcast " + label + " JAR to be " + expectedHash, expectedHash, memberHash.toString());
    }
}
