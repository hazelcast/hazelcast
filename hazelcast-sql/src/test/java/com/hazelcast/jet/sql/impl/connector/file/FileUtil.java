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

package com.hazelcast.jet.sql.impl.connector.file;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

final class FileUtil {

    private static final Record AVRO_RECORD =
            new GenericRecordBuilder(
                    SchemaBuilder.record("name")
                                 .fields()
                                 .name("string").type().stringType().noDefault()
                                 .name("boolean").type().booleanType().noDefault()
                                 .name("byte").type().intType().noDefault()
                                 .name("short").type().intType().noDefault()
                                 .name("int").type().intType().noDefault()
                                 .name("long").type().longType().noDefault()
                                 .name("float").type().floatType().noDefault()
                                 .name("double").type().doubleType().noDefault()
                                 .name("decimal").type().stringType().noDefault()
                                 .name("time").type().stringType().noDefault()
                                 .name("date").type().stringType().noDefault()
                                 .name("timestamp").type().stringType().noDefault()
                                 .name("timestampTz").type().stringType().noDefault()
                                 .name("null").type().nullable().record("nul").fields().endRecord().noDefault()
                                 .name("object").type().record("object").fields().endRecord().noDefault()
                                 .endRecord()
            ).set("string", "string")
             .set("boolean", true)
             .set("byte", (byte) 127)
             .set("short", (short) 32767)
             .set("int", 2147483647)
             .set("long", 9223372036854775807L)
             .set("float", 1234567890.1F)
             .set("double", 123451234567890.1D)
             .set("decimal", "9223372036854775.123")
             .set("time", "12:23:34")
             .set("date", "2020-04-15")
             .set("timestamp", "2020-04-15T12:23:34.001")
             .set("timestampTz", "2020-04-15T12:23:34.200Z")
             .set("null", null)
             .set("object", new GenericRecordBuilder(SchemaBuilder.record("object").fields().endRecord()).build())
             .build();

    private static final Record PARQUET_RECORD =
            new GenericRecordBuilder(
                    SchemaBuilder.record("name")
                                 .fields()
                                 .name("string").type().stringType().noDefault()
                                 .name("boolean").type().booleanType().noDefault()
                                 .name("byte").type().intType().noDefault()
                                 .name("short").type().intType().noDefault()
                                 .name("int").type().intType().noDefault()
                                 .name("long").type().longType().noDefault()
                                 .name("float").type().floatType().noDefault()
                                 .name("double").type().doubleType().noDefault()
                                 .name("decimal").type().stringType().noDefault()
                                 .name("time").type().stringType().noDefault()
                                 .name("date").type().stringType().noDefault()
                                 .name("timestamp").type().stringType().noDefault()
                                 .name("timestampTz").type().stringType().noDefault()
                                 .endRecord()
            ).set("string", "string")
             .set("boolean", true)
             .set("byte", (byte) 127)
             .set("short", (short) 32767)
             .set("int", 2147483647)
             .set("long", 9223372036854775807L)
             .set("float", 1234567890.1F)
             .set("double", 123451234567890.1D)
             .set("decimal", "9223372036854775.123")
             .set("time", "12:23:34")
             .set("date", "2020-04-15")
             .set("timestamp", "2020-04-15T12:23:34.001")
             .set("timestampTz", "2020-04-15T12:23:34.200Z")
             .build();

    private FileUtil() {
    }

    static File createAvroFile() {
        try {
            File file = Files.createTempDirectory("sql-avro-test").toFile();
            file.deleteOnExit();

            try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
                writer.create(AVRO_RECORD.getSchema(), new File(file.getAbsolutePath(), "file.avro"));
                writer.append(AVRO_RECORD);
            }

            return file;
        } catch (IOException ioe) {
            throw sneakyThrow(ioe);
        }
    }

    static byte[] createAvroPayload() {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream();
             DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
            writer.create(AVRO_RECORD.getSchema(), output);
            writer.append(AVRO_RECORD);
            writer.flush();
            return output.toByteArray();
        } catch (IOException ioe) {
            throw sneakyThrow(ioe);
        }
    }

    static void writeParquetPayloadTo(OutputFile file) throws IOException {
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(file)
                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withSchema(PARQUET_RECORD.getSchema())
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withValidation(false)
                .withDictionaryEncoding(false)
                .build()) {
            writer.write(PARQUET_RECORD);
        }
    }
}
