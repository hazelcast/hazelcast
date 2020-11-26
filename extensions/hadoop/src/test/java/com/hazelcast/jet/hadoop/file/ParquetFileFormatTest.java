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
import java.util.Arrays;

public class ParquetFileFormatTest extends BaseFileFormatTest {

    // Parquet has a dependency on Hadoop so it does not make sense to run it without it
    @Parameters(name = "{index}: useHadoop={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(true);
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

    private void createParquetFile() throws IOException {
        Path inputPath = new Path("target/parquet");
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(inputPath, true);
        Path filePath = new Path(inputPath, "file.parquet");

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

        writer.write(new SpecificUser("Frantisek", 7));
        writer.write(new SpecificUser("Ali", 42));
        writer.close();
        fs.close();
    }
}
