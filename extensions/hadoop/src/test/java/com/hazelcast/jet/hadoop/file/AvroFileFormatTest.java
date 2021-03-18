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

import com.hazelcast.jet.hadoop.file.generated.SpecificUser;
import com.hazelcast.jet.hadoop.file.model.IncorrectUser;
import com.hazelcast.jet.hadoop.file.model.User;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import org.apache.avro.AvroTypeException;
import org.apache.avro.InvalidAvroMagicException;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class AvroFileFormatTest extends BaseFileFormatTest {

    @Test
    public void shouldReadAvroWithSchema() throws Exception {
        createAvroFile();

        FileSourceBuilder<SpecificUser> source = FileSources.files(currentDir + "/target/avro")
                                                            .glob("file.avro")
                                                            .format(FileFormat.avro());
        assertItemsInSource(source,
                new SpecificUser("Frantisek", 7),
                new SpecificUser("Ali", 42)
        );
    }

    @Test
    public void shouldReadAvroWithReflection() throws Exception {
        createAvroFile();

        FileSourceBuilder<User> source = FileSources.files(currentDir + "/target/avro")
                                                    .glob("file.avro")
                                                    .format(FileFormat.avro(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }

    @Test
    public void shouldReadEmptyAvroFile() throws Exception {
        createEmptyAvroFile();

        FileSourceBuilder<User> source = FileSources.files(currentDir + "/target/avro")
                                                    .glob("file-empty.avro")
                                                    .format(FileFormat.avro(User.class));

        assertItemsInSource(source, items -> assertThat(items).isEmpty());
    }

    @Test
    public void shouldThrowWhenInvalidFileType() throws Exception {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("invalid-data.png")
                                                    .format(FileFormat.avro(User.class));

        assertJobFailed(source, InvalidAvroMagicException.class, "Not an Avro data file");
    }

    @Test
    public void shouldThrowWhenIncorrectSchema() throws Exception {
        createAvroFile();

        FileSourceBuilder<IncorrectUser> source = FileSources.files(currentDir + "/target/avro")
                                                             .glob("file.avro")
                                                             .format(FileFormat.avro(IncorrectUser.class));

        assertJobFailed(source, AvroTypeException.class, "missing required field");
    }

    private static void createAvroFile() throws IOException {
        createAvroFile("file.avro", new SpecificUser("Frantisek", 7), new SpecificUser("Ali", 42));
    }

    private static void createEmptyAvroFile() throws IOException {
        createAvroFile("file-empty.avro");
    }

    private static void createAvroFile(String filename, SpecificUser... users) throws IOException {
        File target = new File("target/avro");
        FileUtils.deleteDirectory(target);
        target.mkdirs();

        DataFileWriter<SpecificUser> fileWriter = new DataFileWriter<>(new SpecificDatumWriter<>(SpecificUser.class));
        fileWriter.create(SpecificUser.SCHEMA$, new File("target/avro/" + filename));
        for (SpecificUser user : users) {
            fileWriter.append(user);
        }
        fileWriter.close();
    }
}
