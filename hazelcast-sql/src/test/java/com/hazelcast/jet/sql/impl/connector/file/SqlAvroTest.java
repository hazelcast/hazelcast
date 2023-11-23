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

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlService;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.stream.IntStream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_GLOB;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_IGNORE_FILE_NOT_FOUND;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_PATH;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_SHARED_FILE_SYSTEM;
import static java.time.ZoneOffset.UTC;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlAvroTest extends SqlTestSupport {
    private static final File AVRO_FILE = FileUtil.createAvroFile(FileUtil.AVRO_RECORD);
    private static final File AVRO_NULLABLE_FILE = FileUtil.createAvroFile(FileUtil.AVRO_NULLABLE_RECORD);
    private static final File AVRO_NULL_FILE = FileUtil.createAvroFile(FileUtil.AVRO_NULL_RECORD);
    private static final File AVRO_COMPLEX_FILE = FileUtil.createAvroFile(FileUtil.AVRO_COMPLEX_TYPES);

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initializeWithClient(2, null, null);
        sqlService = instance().getSql();
    }

    private static SqlMapping fileMapping(String name, File file) {
        return new SqlMapping(name, FileSqlConnector.class).options(
                OPTION_FORMAT, AVRO_FORMAT,
                OPTION_PATH, file.getParent(),
                OPTION_GLOB, file.getName(),
                OPTION_SHARED_FILE_SYSTEM, true
        );
    }

    @Test
    public void test_nulls() {
        String name = randomName();
        fileMapping(name, AVRO_FILE)
                .fields("nonExistingField VARCHAR")
                .create();

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row((Object) null))
        );
    }

    @Test
    public void test_fieldsMapping() {
        String name = randomName();
        fileMapping(name, AVRO_FILE)
                .fields("id TINYINT EXTERNAL NAME byte",
                        "name VARCHAR EXTERNAL NAME string")
                .create();

        assertRowsAnyOrder(
                "SELECT id, name FROM " + name,
                List.of(new Row((byte) 127, "string"))
        );
    }

    @Test
    public void test_allSqlTypes() {
        String name = randomName();
        fileMapping(name, AVRO_FILE)
                .fields("string VARCHAR",
                        "\"boolean\" BOOLEAN",
                        "byte TINYINT",
                        "short SMALLINT",
                        "\"int\" INT",
                        "long BIGINT",
                        "\"float\" REAL",
                        "\"double\" DOUBLE",
                        "\"decimal\" DECIMAL",
                        "\"time\" TIME",
                        "\"date\" DATE",
                        "\"timestamp\" TIMESTAMP",
                        "timestampTz TIMESTAMP WITH TIME ZONE",
                        "object OBJECT")
                .create();

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row(
                        "string",
                        true,
                        (byte) 127,
                        (short) 32767,
                        2147483647,
                        9223372036854775807L,
                        1234567890.1F,
                        123451234567890.1D,
                        new BigDecimal("9223372036854775.123"),
                        LocalTime.of(12, 23, 34),
                        LocalDate.of(2020, 4, 15),
                        LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000),
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC),
                        new GenericRecordBuilder(SchemaBuilder.record("object").fields().endRecord()).build()
                ))
        );
    }

    @Test
    public void test_complexAvroTypes() {
        String name = randomName();
        fileMapping(name, AVRO_COMPLEX_FILE).create();

        List<Row> rows = List.of(new Row(getAllValues(FileUtil.AVRO_COMPLEX_TYPES)));
        assertRowsAnyOrder(instances()[1], "SELECT * FROM " + name, rows);
        assertRowsAnyOrder(client(), "SELECT * FROM " + name, rows);
    }

    @Test
    public void test_schemaDiscovery() {
        test_schemaDiscovery(AVRO_FILE);
    }

    @Test
    public void test_schemaDiscovery_nullableFields() {
        test_schemaDiscovery(AVRO_NULLABLE_FILE);
    }

    @Test
    public void test_schemaDiscovery_nulls() {
        test_schemaDiscovery(AVRO_NULL_FILE);
    }

    private void test_schemaDiscovery(File avroFile) {
        String name = randomName();
        fileMapping(name, avroFile).create();

        assertRowsAnyOrder(
                "SELECT "
                        + "string"
                        + ", \"boolean\""
                        + ", byte"
                        + ", short"
                        + ", \"int\""
                        + ", long"
                        + ", \"float\""
                        + ", \"double\""
                        + ", \"decimal\""
                        + ", \"time\""
                        + ", \"date\""
                        + ", \"timestamp\""
                        + ", \"timestampTz\""
                        + ", \"null\""
                        + ", object "
                        + "FROM " + name,
                List.of(avroFile == AVRO_NULL_FILE ? new Row(new Object[15]) : new Row(
                        "string",
                        true,
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        1234567890.1F,
                        123451234567890.1D,
                        "9223372036854775.123",
                        "12:23:34",
                        "2020-04-15",
                        "2020-04-15T12:23:34.001",
                        "2020-04-15T12:23:34.200Z",
                        null,
                        new GenericRecordBuilder(SchemaBuilder.record("object").fields().endRecord()).build()
                ))
        );
    }

    @Test
    public void test_tableFunction() {
        test_tableFunction(AVRO_FILE);
    }

    @Test
    public void test_tableFunction_nullableFields() {
        test_tableFunction(AVRO_NULLABLE_FILE);
    }

    @Test
    public void test_tableFunction_nulls() {
        test_tableFunction(AVRO_NULL_FILE);
    }

    private void test_tableFunction(File file) {
        assertRowsAnyOrder(
                "SELECT "
                        + "string"
                        + ", \"boolean\""
                        + ", byte"
                        + ", short"
                        + ", \"int\""
                        + ", long"
                        + ", \"float\""
                        + ", \"double\""
                        + ", \"decimal\""
                        + ", \"time\""
                        + ", \"date\""
                        + ", \"timestamp\""
                        + ", \"timestampTz\""
                        + ", \"null\""
                        + ", object "
                        + "FROM TABLE(" + avroFile(
                                OPTION_PATH, file.getParent(),
                                OPTION_SHARED_FILE_SYSTEM, true)
                        + ")",
                List.of(file == AVRO_NULL_FILE ? new Row(new Object[15]) : new Row(
                        "string",
                        true,
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        1234567890.1F,
                        123451234567890.1D,
                        "9223372036854775.123",
                        "12:23:34",
                        "2020-04-15",
                        "2020-04-15T12:23:34.001",
                        "2020-04-15T12:23:34.200Z",
                        null,
                        new GenericRecordBuilder(SchemaBuilder.record("object").fields().endRecord()).build()
                ))
        );
    }

    @Test
    public void when_conversionFails_then_queryFails() {
        String name = randomName();
        fileMapping(name, AVRO_FILE)
                .fields("string INT")
                .create();

        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM " + name).iterator().hasNext())
                .hasMessageContaining("Cannot parse VARCHAR value to INTEGER");
    }

    @Test
    public void when_columnsSpecified_then_fileNotAccessed() {
        String name = randomName();
        fileMapping(name, new File("/non-existent-directory/foo.avro"))
                .fields("field INT")
                .create();
    }

    @Test
    public void when_fileDoesNotExist_then_fails() {
        String name = randomName();
        assertThatThrownBy(() ->
                fileMapping(name, new File(AVRO_FILE.getParent(), "foo.avro")).create()
        ).hasMessageContaining("matches no files");
    }

    @Test
    public void when_fileDoesNotExistAndIgnoreFileNotFound_then_returnNoResults() {
        String name = randomName();
        fileMapping(name, new File(AVRO_FILE.getParent(), "foo.avro"))
                .fields("field INT")
                .options(OPTION_IGNORE_FILE_NOT_FOUND, true)
                .create();

        assertThat(sqlService.execute("SELECT * FROM " + name).iterator().hasNext())
                .describedAs("no results from non existing file")
                .isFalse();
    }

    @Test
    public void when_directoryDoesNotExist_then_tableFunctionThrowsException() {
        String path = hadoopNonExistingPath();
        assertThatThrownBy(() -> sqlService.execute(
                "SELECT * FROM TABLE(" + avroFile(OPTION_PATH, path) + ")"
        )).hasMessageContaining("The directory '" + path + "' does not exist");
    }

    @Test
    public void when_fileDoesNotExist_then_tableFunctionThrowsException() {
        assertThatThrownBy(() -> sqlService.execute(
                "SELECT * FROM TABLE(" + avroFile(
                        OPTION_PATH, AVRO_FILE.getParent(),
                        OPTION_GLOB, "foo.avro") +
                ")"
        )).hasMessageContaining("matches no files");
    }

    private static String avroFile(Object... params) {
        return IntStream.range(0, params.length / 2)
                .mapToObj(i -> params[2 * i] + " => '" + params[2 * i + 1] + "'")
                .collect(joining(", ", "AVRO_FILE(", ")"));
    }

    private static Object[] getAllValues(GenericRecord record) {
        return IntStream.range(0, record.getSchema().getFields().size())
                .mapToObj(record::get)
                .toArray();
    }
}
