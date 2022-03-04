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
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.CSV_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;

public class SqlPlanCacheTest extends SqlTestSupport {

    private static final String RESOURCES_PATH = Paths.get("src/test/resources").toFile().getAbsolutePath();

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_tableName() {
        createMappingWithGlob("file1", "id", "file.csv");
        sqlService.execute("SELECT * FROM file1");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        createMappingWithGlob("file2", "id", "file.csv");
        sqlService.execute("DROP MAPPING file1");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_fieldList() {
        createMappingWithGlob("file", "id", "file.csv");
        sqlService.execute("SELECT * FROM file");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        createMappingWithGlob("file", "name", "file.csv");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_options() {
        createMappingWithGlob("file", "id", "file.csv");
        sqlService.execute("SELECT * FROM file");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        createMappingWithGlob("file", "id", "*");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_tableFunction() {
        sqlService.execute("SELECT * FROM TABLE(CSV_FILE('" + RESOURCES_PATH + "', 'file.csv'))");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_tableFunctionInAJob() {
        createMapping("map", int.class, String.class);
        instance().getMap("map").put(1, "1");

        sqlService.execute("CREATE JOB job AS "
                + "SINK INTO map SELECT \"int\", string FROM TABLE(JSON_FLAT_FILE('" + RESOURCES_PATH + "', 'file.json'))");
        assertThat(planCache(instance()).size()).isZero();
    }

    private static void createMappingWithGlob(String name, String fieldName, String glob) {
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + " ("
                + fieldName + " TINYINT EXTERNAL NAME byte"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + CSV_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + RESOURCES_PATH + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='" + glob + '\''
                + ")"
        );
    }
}
