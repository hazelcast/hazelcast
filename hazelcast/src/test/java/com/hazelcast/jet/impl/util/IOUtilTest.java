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

package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.test.JetAssert;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.hazelcast.jet.impl.util.IOUtil.packDirectoryIntoZip;
import static com.hazelcast.jet.impl.util.IOUtil.unzip;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IOUtilTest extends JetTestSupport {

    @Rule
    public TestName testName = new TestName();

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

    @Test
    public void when_zipSlipVulnerability_then_zipEntryIgnored_absolutePath() throws Exception {
        when_zipSlipVulnerability_then_zipEntryIgnored(
                (targetDir, nonTargetDir) -> nonTargetDir.resolve("file.txt").resolve("file.txt"));
    }

    @Test
    public void when_zipSlipVulnerability_then_zipEntryIgnored_relativePath() throws Exception {
        when_zipSlipVulnerability_then_zipEntryIgnored(
                (targetDir, nonTargetDir) -> targetDir.relativize(nonTargetDir).resolve("file.txt"));
    }

    @Test
    public void when_zipSlipVulnerability_then_zipEntryIgnored_goingUpALot() throws Exception {
        when_zipSlipVulnerability_then_zipEntryIgnored(
                (targetDir, nonTargetDir) -> Paths.get("..", "..", "..", "..", "..", "..", "..", "..", "file.txt"));
    }

    /**
     * @param entryFunction maps from (targetDir, nonTargetDir) to an entry path
     */
    private void when_zipSlipVulnerability_then_zipEntryIgnored(BiFunction<Path, Path, Path> entryFunction)
            throws Exception {
        Path tmpTargetDir = Files.createTempDirectory(testName.getMethodName());
        Path tmpNonTargetDir = Files.createTempDirectory(testName.getMethodName());
        try {
            byte[] zipFile;
            String entryName = entryFunction.apply(tmpTargetDir, tmpNonTargetDir).toString();
            try (
                    ByteArrayOutputStream baos = new ByteArrayOutputStream(32 * 1024);
                    ZipOutputStream zos = new ZipOutputStream(baos)
            ) {
                zos.putNextEntry(new ZipEntry(entryName));
                zos.write("foo".getBytes());
                zos.closeEntry();
                zos.close();
                zipFile = baos.toByteArray();
            }

            assertThatThrownBy(() -> unzip(new ByteArrayInputStream(zipFile), tmpTargetDir))
                    .hasMessage("Entry with an illegal path: " + entryName);
            assertEquals(0, Files.list(tmpNonTargetDir).count());
        } finally {
            com.hazelcast.internal.nio.IOUtil.delete(tmpTargetDir);
            com.hazelcast.internal.nio.IOUtil.delete(tmpNonTargetDir);
        }
    }
}
