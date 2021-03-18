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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.io.JsonEOFException;
import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.hadoop.file.model.User;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

public class JsonFileFormatTest extends BaseFileFormatTest {

    @Test
    public void shouldReadJsonFile() {
        FileSourceBuilder<Map<String, Object>> source = FileSources.files(currentDir + "/src/test/resources")
                                                         .glob("file.jsonl")
                                                         .format(FileFormat.json());

        assertItemsInSource(source,
                collected -> assertThat(collected).usingRecursiveFieldByFieldElementComparator()
                                                  .containsOnly(
                                                          ImmutableMap.of(
                                                                  "name", "Frantisek",
                                                                  "favoriteNumber", 7
                                                          ),
                                                          ImmutableMap.of(
                                                                  "name", "Ali",
                                                                  "favoriteNumber", 42
                                                          )
                                                  )
        );
    }

    @Test
    public void shouldReadJsonFileToObject() {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file.jsonl")
                                                    .format(FileFormat.json(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }

    @Test
    public void shouldReadPrettyPrintedJsonFile() {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("pretty-printed-file-*.json")
                                                    .format(FileFormat.json(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }

    @Test
    public void shouldReadJsonFileWithMoreAttributesThanTargetClass() {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-more-attributes.jsonl")
                                                    .format(FileFormat.json(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }

    @Test
    public void shouldReadJsonFileWithLessColumnsThanTargetClass() {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-less-attributes.jsonl")
                                                    .format(FileFormat.json(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 0),
                new User("Ali", 0)
        );
    }

    @Test
    public void shouldReadEmptyJsonFile() {

        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-empty.json")
                                                    .format(FileFormat.json(User.class));

        assertItemsInSource(source, items -> assertThat(items).isEmpty());
    }

    @Test
    public void shouldThrowWhenInvalidFileType() {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("invalid-data.png")
                                                    .format(FileFormat.json(User.class));

        assertJobFailed(source, JsonParseException.class, "Unexpected character");
    }

    @Test
    public void shouldThrowWhenWrongFormatting() {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-invalid.jsonl")
                                                    .format(FileFormat.json(User.class));

        assertJobFailed(source, JsonParseException.class, "Unexpected character");
    }

    @Test
    public void shouldReadFileWithRecordsOnMultipleLines() {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-multiline.jsonl")
                                                    .format(FileFormat.json(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }

    @Test
    public void shouldFailReadingFileWithRecordsOnMultipleLinesWhenMultilineOff() {
        assumeThat(useHadoop)
                .describedAs("multiline(false) has no effect for local connector")
                .isTrue();

        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-multiline.jsonl")
                                                    .format(FileFormat.json(User.class).multiline(false));

        assertJobFailed(source, JsonEOFException.class, "expected close marker for Object");
    }
}
