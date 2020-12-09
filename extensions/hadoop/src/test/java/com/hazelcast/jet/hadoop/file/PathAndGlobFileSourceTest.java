/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.hadoop.file;

import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assumptions.assumeThat;

public class PathAndGlobFileSourceTest extends BaseFileFormatTest {

    @Test
    public void shouldReadFilesMatchingGlob() {
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/resources/glob")
                                                      .glob("file*")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, "file", "file1");
    }

    @Test
    public void shouldReadFilesMatchingGlobInTheMiddle() {
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/resources/glob")
                                                      .glob("f*le")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, "file");
    }

    @Test
    public void shouldReadFilesMatchingGlobInPath() {
        assumeThat(useHadoop).isTrue();
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/*/glob") // src/test/resources/glob
                                                      .glob("file")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, "file");
    }

    @Test
    public void shouldReadFilesMatchingGlobInPath_moreDirs() {
        assumeThat(useHadoop).isTrue();
        // src/test/resources/globpath1/globpath and src/test/resources/globpath2/globpath
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/resources/*/globpath")
                                                      .glob("file")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, "fileA", "fileB");
    }

    @Test
    public void shouldNotReadFilesMatchingGlobInPathForDirChain() {
        assumeThat(useHadoop).isTrue();
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/*/glob") // src/test/resources/glob
                                                      .glob("file")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, items -> assertThat(items).isEmpty());
    }

    @Test
    public void shouldReadFilesMatchingGlobInPathAndGlob() {
        assumeThat(useHadoop).isTrue();
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/*/glob") // src/test/resources/glob
                                                      .glob("file*")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, "file", "file1");
    }

    @Test
    public void shouldReadFileWithEscapedGlob() throws IOException {
        assumeThatNoWindowsOS(); // * is not allowed in filename

        try (PrintWriter out = new PrintWriter("target/file*")) {
            out.print("file*");
        }

        FileSourceBuilder<String> source = FileSources.files(currentDir + "/target")
                                                      .glob("file\\*")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, "file*");

        source = FileSources.files(currentDir + "/target")
                            .glob("file*")
                            .format(FileFormat.text());

        assertItemsInSource(source, "file*");
    }

    @Test
    public void shouldReadAllFilesInDirectory() {
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/resources/directory/")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, (collected) -> assertThat(collected).hasSize(2));
    }

    @Test
    public void shouldReadAllFilesInDirectoryNoSlash() {
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/resources/directory")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, (collected) -> assertThat(collected).hasSize(2));
    }

    @Test
    public void shouldReadAllFilesInDirectoryWithNativeSeparator() {
        String path = currentDir + File.separator +
                "src" + File.separator + "test" + File.separator + "resources" + File.separator + "directory";
        FileSourceBuilder<String> source = FileSources.files(path)
                                                      .format(FileFormat.text());

        assertItemsInSource(source, (collected) -> assertThat(collected).hasSize(2));
    }

    @Test
    public void shouldIgnoreSubdirectories() {
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/resources/level1")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, "level1_file");
    }

    @Test
    public void shouldIgnoreSubdirectoriesWhenUsingGlob() {
        assumeThat(useHadoop).isFalse();
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/resources/level1/")
                                                      .glob("*")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, "level1_file");
    }

    @Test
    public void shouldReadPathNoDirectoryFileOnly() {
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/.")
                                                      .glob("pom.xml")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, (collected) ->
                assertThat(collected).anyMatch(s -> s.contains("<artifactId>hazelcast-jet-hadoop</artifactId>"))
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAcceptRelativePath() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, (items) -> fail("should have thrown exception"));
    }

    @Test
    public void shouldNotReadAnyFileForNonMatchingGlob() {
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/resources/glob")
                                                      .glob("doesnotmatch")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, items -> assertThat(items).isEmpty());
    }

    @Test
    public void shouldNotReadAnyFileForNonExistingFolder() {
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/resources/notexists")
                                                      .glob("*")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, items -> assertThat(items).isEmpty());
    }
}
