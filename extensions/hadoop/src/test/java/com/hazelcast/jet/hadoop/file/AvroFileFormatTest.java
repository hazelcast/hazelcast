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

import com.hazelcast.jet.hadoop.file.generated.SpecificUser;
import com.hazelcast.jet.hadoop.file.model.User;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

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

    private static void createAvroFile() throws IOException {
        File target = new File("target/avro");
        FileUtils.deleteDirectory(target);
        target.mkdirs();

        DataFileWriter<SpecificUser> fileWriter = new DataFileWriter<>(new SpecificDatumWriter<>(SpecificUser.class));
        fileWriter.create(SpecificUser.SCHEMA$, new File("target/avro/file.avro"));
        fileWriter.append(new SpecificUser("Frantisek", 7));
        fileWriter.append(new SpecificUser("Ali", 42));
        fileWriter.close();
    }
}
