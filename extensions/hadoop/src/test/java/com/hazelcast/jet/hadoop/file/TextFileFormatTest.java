/*
 * Copyright 2021 Hazelcast Inc.
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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;


public class TextFileFormatTest extends BaseFileFormatTest {

    @Test
    public void readTextFileAsSingleItem() throws Exception {
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/resources")
                                                      .glob("file.txt")
                                                      .format(FileFormat.text());

        String expected = new String(
                Files.readAllBytes(Paths.get(currentDir, "/src/test/resources", "file.txt")),
                StandardCharsets.UTF_8);

        assertItemsInSource(source, expected);
    }

    @Test
    public void readTextFileAsLines() {
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/resources")
                                                      .glob("file.txt")
                                                      .format(FileFormat.lines());

        assertItemsInSource(source, "Text contents of", "the file.");
    }

    @Test
    public void shouldReadEmptyTextFile() throws Exception {
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/resources")
                                                      .glob("file-empty.txt")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, "");
    }

    @Test
    public void shouldReadEmptyLinesTextFile() throws Exception {
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/resources")
                                                      .glob("file-empty.txt")
                                                      .format(FileFormat.lines());

        assertItemsInSource(source, items -> assertThat(items).isEmpty());
    }

    @Test
    public void shouldReadTextFileWithCharset() {
        // Charset isn't available on Hadoop - all text is in UTF-8
        assumeThat(useHadoop).isFalse();

        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/resources")
                                                      .glob("cp1250.txt")
                                                      .format(FileFormat.text(Charset.forName("Cp1250")));

        assertItemsInSource(source, "Příliš žluťoučký kůň úpěl ďábelské ódy.");
    }

    @Test
    public void defaultFileFormatShouldReadFileAsLines() {
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/resources")
                                                      .glob("file.txt");

        assertItemsInSource(source, "Text contents of", "the file.");
    }
}
