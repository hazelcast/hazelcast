/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.test.JetAssert;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.IOUtil.packDirectoryIntoZip;
import static com.hazelcast.jet.impl.util.IOUtil.unzip;

@RunWith(HazelcastSerialClassRunner.class)
public class IOUtilTest extends JetTestSupport {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void test_zipAndUnzipNestedFolder_then_contentsShouldBeSame() throws Exception {
        Path originalPath = Paths.get(this.getClass().getResource("/nested").toURI());
        test(originalPath);
    }

    @Test
    public void test_zipAndUnzipNestedFoldersWithAnEmptySubFolder_then_contentsShouldBeSame() throws Exception {
        Path originalPath = Paths.get(this.getClass().getResource("/nested").toURI());
        Path randomFile = originalPath.resolve(randomName());
        Files.createDirectory(randomFile);
        try {
            test(originalPath);
        } finally {
            com.hazelcast.internal.nio.IOUtil.delete(randomFile);
        }
    }

    public void test(Path originalPath) throws IOException {
        //Given
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);

        //When
        packDirectoryIntoZip(originalPath, baos);
        Path unzippedPath = temporaryFolder.newFolder().toPath();
        unzip(new ByteArrayInputStream(baos.toByteArray()), unzippedPath);

        //Then
        Set<Path> originalSet = getChildrenFromRoot(originalPath);
        Set<Path> unzippedSet = getChildrenFromRoot(unzippedPath);

        assertCollection(originalSet, unzippedSet);

        boolean allMatch = Files.walk(unzippedPath)
                                .filter(Files::isRegularFile)
                                .allMatch(p -> {
                                    try {
                                        String line = Files.lines(p).iterator().next();
                                        return line.equals(p.getFileName().toString() + " content");
                                    } catch (IOException e) {
                                        throw ExceptionUtil.rethrow(e);
                                    }
                                });
        JetAssert.assertTrue("File contents are not matching", allMatch);
    }

    private Set<Path> getChildrenFromRoot(Path path) throws IOException {
        return Files.walk(path)
                    .map(path::relativize)
                    .filter(p -> !p.toString().isEmpty())
                    .collect(Collectors.toSet());
    }
}
