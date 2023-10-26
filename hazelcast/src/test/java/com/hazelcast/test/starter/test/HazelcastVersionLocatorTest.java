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

package com.hazelcast.test.starter.test;

import static com.hazelcast.test.starter.HazelcastVersionLocator.Artifact.EE_JAR;
import static com.hazelcast.test.starter.HazelcastVersionLocator.Artifact.OS_JAR;
import static com.hazelcast.test.starter.HazelcastVersionLocator.Artifact.OS_TEST_JAR;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.hazelcast.test.starter.HazelcastVersionLocator;
import com.hazelcast.test.starter.HazelcastVersionLocator.Artifact;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.stream.Stream;

/**
 * TODO This test doesn't force a re-download, so if an artifact is cached in the local repository, the download won't be
 * exercised. It's difficult to modify the local Maven repository as it's not encapsulated for the scope of testing
 */
public class HazelcastVersionLocatorTest {
    private static HashFunction hashFunction;
    private static Map<HazelcastVersionLocator.Artifact, File> files;

    @BeforeAll
    public static void setUp() {
        hashFunction = Hashing.crc32c();
        files = HazelcastVersionLocator.locateVersion("4.0", true);
    }

    static Stream<Arguments> testDownloadVersion() {
        return Stream.of(Arguments.of(OS_JAR, "4db18099"), Arguments.of(OS_TEST_JAR, "80f97565"),
                Arguments.of(EE_JAR, "806220c1"));
    }

    @ParameterizedTest
    @MethodSource("testDownloadVersion")
    void testDownloadVersion(Artifact artifact, String expectedHash) throws IOException {
        final File file = files.get(artifact);
        final HashCode memberHash = Files.asByteSource(file).hash(hashFunction);
        assertEquals(expectedHash, memberHash.toString(),
                MessageFormat.format("Expected hash of Hazelcast {0} JAR to be {1}", artifact, expectedHash));
    }
}
