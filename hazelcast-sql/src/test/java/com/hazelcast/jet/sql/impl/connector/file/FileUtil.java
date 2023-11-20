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

package com.hazelcast.jet.sql.impl.connector.file;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.reduce;
import static org.apache.avro.generic.GenericData.STRING_PROP;
import static org.apache.avro.generic.GenericData.StringType.String;

final class FileUtil {

    static final GenericRecord AVRO_RECORD =
            new GenericRecordBuilder(SchemaBuilder.record("name")
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
             .set("byte", (int) Byte.MAX_VALUE)
             .set("short", (int) Short.MAX_VALUE)
             .set("int", Integer.MAX_VALUE)
             .set("long", Long.MAX_VALUE)
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

    static final GenericRecord AVRO_NULLABLE_RECORD = reduce(
            new GenericRecordBuilder(SchemaBuilder.record("name")
                    .fields()
                    .name("string").type().nullable().stringType().noDefault()
                    .name("boolean").type().nullable().booleanType().noDefault()
                    .name("byte").type().nullable().intType().noDefault()
                    .name("short").type().nullable().intType().noDefault()
                    .name("int").type().nullable().intType().noDefault()
                    .name("long").type().nullable().longType().noDefault()
                    .name("float").type().nullable().floatType().noDefault()
                    .name("double").type().nullable().doubleType().noDefault()
                    .name("decimal").type().nullable().stringType().noDefault()
                    .name("time").type().nullable().stringType().noDefault()
                    .name("date").type().nullable().stringType().noDefault()
                    .name("timestamp").type().nullable().stringType().noDefault()
                    .name("timestampTz").type().nullable().stringType().noDefault()
                    .name("null").type().nullable().record("nul").fields().endRecord().noDefault()
                    .name("object").type().nullable().record("object").fields().endRecord().noDefault()
                    .endRecord()),
            AVRO_RECORD.getSchema().getFields().stream(),
            (record, field) -> record.set(field, AVRO_RECORD.get(field.pos()))
    ).build();

    static final GenericRecord AVRO_NULL_RECORD = reduce(
            new GenericRecordBuilder(AVRO_NULLABLE_RECORD.getSchema()),
            AVRO_NULLABLE_RECORD.getSchema().getFields().stream(),
            (record, field) -> record.set(field, null)
    ).build();

    static final GenericRecord AVRO_COMPLEX_TYPES;
    static {
        Schema mapSchema = SchemaBuilder.map().prop(STRING_PROP, String).values(Schema.create(Schema.Type.INT));
        Schema recordSchema = SchemaBuilder.record("record").fields().requiredInt("field").endRecord();
        Schema arraySchema = SchemaBuilder.array().items(Schema.create(Schema.Type.INT));
        Schema enumSchema = SchemaBuilder.enumeration("enum").symbols("symbol");
        Schema fixedSchema = SchemaBuilder.fixed("fixed").size(1);

        AVRO_COMPLEX_TYPES = new GenericRecordBuilder(SchemaBuilder.record("complex")
                .fields()
                .requiredBytes("bytes")
                .name("map").type(mapSchema).noDefault()
                .name("record").type(recordSchema).noDefault()
                .name("array").type(arraySchema).noDefault()
                .name("enum").type(enumSchema).noDefault()
                .name("fixed").type(fixedSchema).noDefault()
                .endRecord()
        ).set("bytes", ByteBuffer.wrap(new byte[]{(byte) 19}))
         .set("map", Map.of("key", 71))
         .set("record", new GenericRecordBuilder(recordSchema).set("field", 23).build())
         .set("array", new GenericData.Array<>(arraySchema, List.of(53)))
         .set("enum", new GenericData.EnumSymbol(enumSchema, "symbol"))
         .set("fixed", new GenericData.Fixed(fixedSchema, new byte[]{(byte) 74}))
         .build();
    }

    private static final GenericRecord PARQUET_RECORD =
            new GenericRecordBuilder(SchemaBuilder.record("name")
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
             .set("byte", Byte.MAX_VALUE)
             .set("short", Short.MAX_VALUE)
             .set("int", Integer.MAX_VALUE)
             .set("long", Long.MAX_VALUE)
             .set("float", 1234567890.1F)
             .set("double", 123451234567890.1D)
             .set("decimal", "9223372036854775.123")
             .set("time", "12:23:34")
             .set("date", "2020-04-15")
             .set("timestamp", "2020-04-15T12:23:34.001")
             .set("timestampTz", "2020-04-15T12:23:34.200Z")
             .build();

    private FileUtil() { }

    /**
     * Creates a temporary directory with prefix 'sql-avro-test', writes the
     * specified Avro record to 'file.avro' in this directory and returns the file.
     */
    static File createAvroFile(GenericRecord avroRecord) {
        try {
            File directory = Files.createTempDirectory("sql-avro-test").toFile();
            directory.deleteOnExit();
            File file = new File(directory, "file.avro");

            try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
                writer.create(avroRecord.getSchema(), file);
                writer.append(avroRecord);
            }
            return file;
        } catch (IOException e) {
            throw sneakyThrow(e);
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
