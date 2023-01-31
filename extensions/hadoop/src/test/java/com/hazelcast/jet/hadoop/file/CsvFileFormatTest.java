/*
 * Copyright 2023 Hazelcast Inc.
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

import com.fasterxml.jackson.dataformat.csv.CsvReadException;
import com.hazelcast.jet.hadoop.file.model.User;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import org.junit.Test;

import java.io.CharConversionException;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

public class CsvFileFormatTest extends BaseFileFormatTest {

    @Test
    public void shouldReadCsvFile() {
        FileSourceBuilder<String[]> source = FileSources.files(currentDir + "/src/test/resources")
                                                        .glob("file.csv")
                                                        .format(FileFormat.csv(asList("name", "favoriteNumber")));

        assertItemsInSource(source,
                new String[]{"Frantisek", "7"},
                new String[]{"Ali", "42"}
        );
    }

    @Test
    public void shouldRemapFields() {
        FileSourceBuilder<String[]> source = FileSources.files(currentDir + "/src/test/resources")
                                                        .glob("file.csv")
                                                        .format(FileFormat.csv(asList("favoriteNumber", "name")));

        assertItemsInSource(source,
                new String[]{"7", "Frantisek"},
                new String[]{"42", "Ali"}
        );
    }

    @Test
    public void shouldIgnoreEmptyLine() {
        FileSourceBuilder<String[]> source = FileSources.files(currentDir + "/src/test/resources")
                                                        .glob("file-empty-line.csv")
                                                        .format(FileFormat.csv(asList("name", "favoriteNumber")));

        assertItemsInSource(source,
                new String[]{"Frantisek", "7"},
                new String[]{"Ali", "42"}
        );
    }

    @Test
    public void shouldRemapSubsetOfFields() {
        FileSourceBuilder<String[]> source = FileSources.files(currentDir + "/src/test/resources")
                                                        .glob("file.csv")
                                                        .format(FileFormat.csv(singletonList("name")));

        assertItemsInSource(source,
                new String[]{"Frantisek"},
                new String[]{"Ali"}
        );
    }

    @Test
    public void shouldReadCsvFileToObject() {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file.csv")
                                                    .format(FileFormat.csv(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }

    @Test
    public void shouldReadCsvToObjectIgnoreEmptyLine() {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-empty-line.csv")
                                                    .format(FileFormat.csv(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }

    @Test
    public void shouldReadCsvFileWithMoreColumnsThanTargetClass() {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-more-columns.csv")
                                                    .format(FileFormat.csv(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }

    @Test
    public void shouldReadCsvFileWithLessColumnsThanTargetClass() {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-less-columns.csv")
                                                    .format(FileFormat.csv(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 0),
                new User("Ali", 0)
        );
    }

    @Test
    public void shouldReadEmptyCsvFile() {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-empty.csv")
                                                    .format(FileFormat.csv(User.class));

        assertItemsInSource(source, items -> assertThat(items).isEmpty());
    }

    @Test
    public void shouldThrowWhenInvalidFileType() {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("invalid-data.png")
                                                    .format(FileFormat.csv(User.class));

        assertJobFailed(source, CharConversionException.class, "Invalid UTF-8");
    }

    @Test
    public void shouldThrowWhenWrongFormatting() {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-invalid.csv")
                                                    .format(FileFormat.csv(User.class));

        assertJobFailed(source, CsvReadException.class, "Too many entries");
    }

    @Test
    public void shouldReadLargeCsvFile() throws Exception {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-200-entries-with-header.csv")
                                                    .sharedFileSystem(true)
                                                    .option("mapreduce.input.fileinputformat.split.minsize", "32")
                                                    .option("mapreduce.input.fileinputformat.split.maxsize", "64")
                                                    .format(FileFormat.csv(User.class));

        assertItemsInSource(source, (collected) -> assertThat(collected).hasSize(200));
    }

    @Test
    public void shouldReadLargeCsvFileCompressed() throws Exception {
        assumeThat(useHadoop).isTrue();

        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-200-entries-with-header.csv.gz")
                                                    .sharedFileSystem(true)
                                                    .option("mapreduce.input.fileinputformat.split.minsize", "32")
                                                    .option("mapreduce.input.fileinputformat.split.maxsize", "64")
                                                    .format(FileFormat.csv(User.class));

        assertItemsInSource(source, (collected) -> assertThat(collected).hasSize(200));
    }

    @Test
    public void shouldReadLargeCsvFileCompressedSplittable() throws Exception {
        assumeThat(useHadoop).isTrue();

        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-200-entries-with-header.csv.bz2")
                                                    .sharedFileSystem(true)
                                                    .option("mapreduce.input.fileinputformat.split.minsize", "32")
                                                    .option("mapreduce.input.fileinputformat.split.maxsize", "64")
                                                    .format(FileFormat.csv(User.class));

        assertItemsInSource(source, (collected) -> assertThat(collected).hasSize(200));
    }
}
