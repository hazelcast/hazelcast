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

import com.fasterxml.jackson.dataformat.csv.CsvMappingException;
import com.hazelcast.jet.hadoop.file.model.User;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import org.junit.Test;

import java.io.CharConversionException;

import static org.assertj.core.api.Assertions.assertThat;

public class CsvFileFormatTest extends BaseFileFormatTest {

    @Test
    public void shouldReadCsvFile() throws Exception {

        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file.csv")
                                                    .format(FileFormat.csv(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }

    @Test
    public void shouldReadCsvFileWithMoreColumnsThanTargetClass() throws Exception {

        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-more-columns.csv")
                                                    .format(FileFormat.csv(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }

    @Test
    public void shouldReadCsvFileWithLessColumnsThanTargetClass() throws Exception {

        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-less-columns.csv")
                                                    .format(FileFormat.csv(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 0),
                new User("Ali", 0)
        );
    }

    @Test
    public void shouldReadEmptyCsvFile() throws Exception {

        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-empty.csv")
                                                    .format(FileFormat.csv(User.class));

        assertItemsInSource(source, items -> assertThat(items).isEmpty());
    }

    @Test
    public void shouldThrowWhenInvalidFileType() throws Exception {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("invalid-data.png")
                                                    .format(FileFormat.csv(User.class));

        assertJobFailed(source, CharConversionException.class, "Invalid UTF-8");
    }

    @Test
    public void shouldThrowWhenWrongFormatting() throws Exception {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-invalid.csv")
                                                    .format(FileFormat.csv(User.class));

        assertJobFailed(source, CsvMappingException.class, "Too many entries");
    }
}
