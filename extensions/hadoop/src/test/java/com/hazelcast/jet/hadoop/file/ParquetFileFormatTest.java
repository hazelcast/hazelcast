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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.hadoop.file.generated.SpecificUser;
import com.hazelcast.jet.hadoop.file.model.IncorrectUser;
import com.hazelcast.jet.hadoop.file.model.User;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ParquetFileFormatTest extends BaseFileFormatTest {

    // Parquet has a dependency on Hadoop so it does not make sense to run it without it
    @Parameters(name = "{index}: useHadoop={0}")
    public static Iterable<?> parameters() {
        return Collections.singletonList(true);
    }

    @Test
    public void shouldReadParquetFile() throws Exception {
        createParquetFile();

        FileSourceBuilder<SpecificUser> source = FileSources.files(currentDir + "/target/parquet")
                                                            .glob("file.parquet")
                                                            .format(FileFormat.parquet());
        assertItemsInSource(source,
                new SpecificUser("Frantisek", 7),
                new SpecificUser("Ali", 42)
        );
    }

    @Test
    public void shouldReadEmptyParquetFile() throws Exception {
        createEmptyParquetFile();

        FileSourceBuilder<User> source = FileSources.files(currentDir + "/target/parquet")
                                                    .glob("file-empty.parquet")
                                                    .format(FileFormat.parquet());

        assertItemsInSource(source, items -> assertThat(items).isEmpty());
    }

    @Test
    public void shouldThrowWhenInvalidFileType() throws Exception {
        FileSourceBuilder<SpecificUser> source = FileSources.files(currentDir + "/src/test/resources")
                                                            .glob("invalid-data.png")
                                                            .format(FileFormat.parquet());

        assertJobFailed(source, RuntimeException.class, "is not a Parquet file");
    }

    @Test
    public void shouldThrowWhenIncorrectSchema() throws Exception {
        createParquetFile();

        FileSourceBuilder<IncorrectUser> source = FileSources.files(currentDir + "/target/parquet")
                                                             .glob("file.parquet")
                                                             .format(FileFormat.parquet());

        if (useHadoop) {
            source.useHadoopForLocalFiles(true);
        }

        Pipeline p = Pipeline.create();

        // The type of the objects in the pipeline is not enforced at runtime.
        // Adding a mapping step causes explicit cast, resulting in a failed
        // job.
        p.readFrom(source.build())
         .map(IncorrectUser::getSurname)
         .writeTo(Sinks.logger());

        HazelcastInstance[] instances = createHazelcastInstances(1);

        try {
            assertThatThrownBy(() -> instances[0].getJet().newJob(p).join())
                    .hasCauseInstanceOf(JetException.class)
                    .hasRootCauseInstanceOf(ClassCastException.class)
                    .hasMessageContaining("com.hazelcast.jet.hadoop.file.generated.SpecificUser")
                    .hasMessageContaining("com.hazelcast.jet.hadoop.file.model.IncorrectUser");
        } finally {
            for (HazelcastInstance instance : instances) {
                instance.shutdown();
            }
        }
    }

    @Test
    public void shouldReadWithProjection() throws Exception {
        createParquetFile();

        String schema = "{" +
                "  \"type\": \"record\"," +
                "  \"name\": \"SpecificUser\"," +
                "  \"namespace\": \"com.hazelcast.jet.hadoop.file.generated\"," +
                "  \"fields\": [" +
                "    {" +
                "      \"name\": \"name\"," +
                "      \"type\": \"string\"" +
                "    }" +
                "  ]" +
                "}";

        FileSourceBuilder<SpecificUser> source = FileSources.files(currentDir + "/target/parquet")
                                                            .glob("file.parquet")
                                                            .option("parquet.avro.projection", schema)
                                                            .format(FileFormat.parquet());

        assertItemsInSource(source,
                new SpecificUser("Frantisek", null),
                new SpecificUser("Ali", null)
        );
    }

    private void createParquetFile() throws IOException {
        createParquetFile("file.parquet", new SpecificUser("Frantisek", 7), new SpecificUser("Ali", 42));
    }

    private void createEmptyParquetFile() throws IOException {
        createParquetFile("file-empty.parquet");
    }

    private void createParquetFile(String filename, SpecificUser... users) throws IOException {
        Path inputPath = new Path("target/parquet");
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(inputPath, true);
        Path filePath = new Path(inputPath, filename);

        ParquetWriter<SpecificUser> writer = AvroParquetWriter.
                <SpecificUser>builder(filePath)
                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withSchema(SpecificUser.SCHEMA$)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withValidation(false)
                .withDictionaryEncoding(false)
                .build();

        for (SpecificUser user : users) {
            writer.write(user);
        }
        writer.close();
        fs.close();
    }
}
