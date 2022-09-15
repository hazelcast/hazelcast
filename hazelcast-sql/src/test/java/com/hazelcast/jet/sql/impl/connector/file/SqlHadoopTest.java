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

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.CSV_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_FLAT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PARQUET_FORMAT;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlHadoopTest extends SqlTestSupport {

    private static MiniDFSCluster cluster;
    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() throws Exception {
        assumeThatNoWindowsOS();

        // Tests will fail on IBM JDK17 with error:
        // No LoginModule found for com.ibm.security.auth.module.JAASLoginModule
        // see https://github.com/hazelcast/hazelcast/issues/20754
        assumeThatNotIBMJDK17();

        initialize(1, null);
        sqlService = instance().getSql();

        File directory = Files.createTempDirectory("sql-test-hdfs").toFile().getAbsoluteFile();
        directory.deleteOnExit();

        Configuration configuration = new Configuration();
        configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, directory.getAbsolutePath());
        cluster = new MiniDFSCluster.Builder(configuration).build();
        cluster.waitClusterUp();
    }

    @AfterClass
    public static void afterClass() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @Test
    public void test_csv() throws IOException {
        store("/csv/file.csv", "id,name\n1,Alice\n2,Bob");

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id BIGINT"
                + ", name VARCHAR"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + CSV_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("csv") + '\''
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT name, id FROM " + name,
                asList(
                        new Row("Alice", 1L)
                        , new Row("Bob", 2L)
                )
        );
    }

    @Test
    public void test_csvSchemaDiscovery() throws IOException {
        store("/discovered-csv/file.csv", "id,name\n1,Alice\n2,Bob");

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + CSV_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("discovered-csv") + '\''
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT name, id FROM " + name,
                asList(
                        new Row("Alice", "1")
                        , new Row("Bob", "2")
                )
        );
    }

    @Test
    public void test_json() throws IOException {
        store("/json/file.json", "{\"id\": 1, \"name\": \"Alice\"}\n{\"id\": 2, \"name\": \"Bob\"}");

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id BIGINT"
                + ", name VARCHAR"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("json") + '\''
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT name, id FROM " + name,
                asList(
                        new Row("Alice", 1L)
                        , new Row("Bob", 2L)
                )
        );
    }

    @Test
    public void test_jsonSchemaDiscovery() throws IOException {
        store("/discovered-json/file.json", "{\"id\": 1, \"name\": \"Alice\"}\n{\"id\": 2, \"name\": \"Bob\"}");

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("discovered-json") + '\''
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT name, id FROM " + name,
                asList(
                        new Row("Alice", 1D)
                        , new Row("Bob", 2D)
                )
        );
    }

    @Test
    public void test_avro() throws IOException {
        store("/avro/file.avro", FileUtil.createAvroPayload());

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id BIGINT EXTERNAL NAME long"
                + ", name VARCHAR EXTERNAL NAME string"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("avro") + '\''
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(9223372036854775807L, "string"))
        );
    }

    @Test
    public void test_avroSchemaDiscovery() throws IOException {
        store("/discovered-avro/file.avro", FileUtil.createAvroPayload());

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("discovered-avro") + '\''
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT byte, string FROM " + name,
                singletonList(new Row(127, "string"))
        );
    }

    @Test
    public void test_parquet_nulls() throws IOException {
        storeParquet("/parquet-nulls/file.parquet");

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "nonExistingField VARCHAR"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + PARQUET_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("parquet-nulls") + '\''
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row((Object) null))
        );
    }

    @Test
    public void test_parquet_fieldsMapping() throws IOException {
        storeParquet("/parquet-fields-mapping/file.parquet");

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id TINYINT EXTERNAL NAME byte"
                + ", name VARCHAR EXTERNAL NAME string"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + PARQUET_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("parquet-fields-mapping") + '\''
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT id, name FROM " + name,
                singletonList(new Row((byte) 127, "string"))
        );
    }

    @Test
    public void test_parquet_allTypes() throws IOException {
        storeParquet("/parquet-all-types/file.parquet");

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "string VARCHAR"
                + ", \"boolean\" BOOLEAN"
                + ", byte TINYINT"
                + ", short SMALLINT"
                + ", \"int\" INT"
                + ", long BIGINT"
                + ", \"float\" REAL"
                + ", \"double\" DOUBLE"
                + ", \"decimal\" DECIMAL"
                + ", \"time\" TIME"
                + ", \"date\" DATE"
                + ", \"timestamp\" TIMESTAMP"
                + ", timestampTz TIMESTAMP WITH TIME ZONE"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_FORMAT + "'='" + PARQUET_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("parquet-all-types") + '\''
                + ")"
        );

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(
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
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC)
                ))
        );
    }

    @Test
    public void test_parquet_schemaDiscovery() throws IOException {
        storeParquet("/parquet-schema-discovery/file.parquet");

        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_FORMAT + "'='" + PARQUET_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("parquet-schema-discovery") + '\''
                + ")"
        );

        assertRowsAnyOrder("SELECT "
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
                        + " FROM " + name,
                singletonList(new Row(
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
                        "2020-04-15T12:23:34.200Z"
                ))
        );
    }

    @Test
    public void test_parquet_tableFunction() throws IOException {
        storeParquet("/parquet-table-function/file.parquet");

        assertRowsAnyOrder("SELECT "
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
                        + " FROM TABLE ("
                        + "PARQUET_FILE ('" + path("parquet-table-function") + "')"
                        + ")",
                singletonList(new Row(
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
                        "2020-04-15T12:23:34.200Z"
                ))
        );
    }

    @Test
    public void when_fileDoesNotExist_thenThrowException() {
        String name = randomName();

        assertThatThrownBy(() ->
                sqlService.execute("CREATE MAPPING " + name
                        + " TYPE " + FileSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + '\'' + OPTION_FORMAT + "'='" + CSV_FORMAT + '\''
                        + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("") + '\''
                        + ", '" + FileSqlConnector.OPTION_GLOB + "'='" + "foo.csv" + '\''
                        + ")"
                )
        ).hasMessageContaining("matches no files");
    }

    @Test
    public void when_fileDoesNotExistAndIgnoreFileNotFound_thenReturnNoResults() throws IOException {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " (field INT) "
                + " TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_FORMAT + "'='" + CSV_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + path("") + '\''
                + ", '" + FileSqlConnector.OPTION_IGNORE_FILE_NOT_FOUND + "'='" + "true" + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='" + "foo.csv" + '\''
                + ")"
        );

        assertThat(sqlService.execute("SELECT * FROM " + name).iterator().hasNext())
                .describedAs("no results from non existing file")
                .isFalse();
    }

    @Test
    public void when_fileDoesNotExist_thenTableFunctionThrowsException() {
        assertThatThrownBy(() -> sqlService.execute(
                "SELECT * "
                        + " FROM TABLE ("
                        + "csv_file ("
                        + " path => '" + path("") + "'"
                        + " , glob => 'foo.csv'"
                        + " , options => MAP['key', 'value']"
                        + ")"
                        + ")")
        ).hasMessageContaining("matches no files");
    }

    private static String path(String suffix) throws IOException {
        return cluster.getFileSystem().getUri() + "/" + suffix;
    }

    private static void store(String path, String content) throws IOException {
        try (FSDataOutputStream output = cluster.getFileSystem().create(new Path(path))) {
            output.writeBytes(content);
        }
    }

    private static void store(String path, byte[] content) throws IOException {
        try (FSDataOutputStream output = cluster.getFileSystem().create(new Path(path))) {
            output.write(content);
        }
    }

    private static void storeParquet(String path) throws IOException {
        OutputFile file = HadoopOutputFile.fromPath(new Path(path), cluster.getFileSystem().getConf());
        FileUtil.writeParquetPayloadTo(file);
    }
}
