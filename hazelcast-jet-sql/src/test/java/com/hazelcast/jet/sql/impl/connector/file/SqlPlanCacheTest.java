/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
        createMapping("file1", "id", "file.csv");
        sqlService.execute("SELECT * FROM file1");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        createMapping("file2", "id", "file.csv");
        sqlService.execute("DROP MAPPING file1");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_fieldList() {
        createMapping("file", "id", "file.csv");
        sqlService.execute("SELECT * FROM file");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        createMapping("file", "name", "file.csv");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_options() {
        createMapping("file", "id", "file.csv");
        sqlService.execute("SELECT * FROM file");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        createMapping("file", "id", "*");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_tableFunction() {
        sqlService.execute("SELECT * FROM TABLE(CSV_FILE('" + RESOURCES_PATH + "', 'file.csv'))");
        assertThat(planCache(instance()).size()).isZero();
    }

    @Test
    public void test_tableFunctionInAJob() {
        instance().getMap("map").put(1, "1");

        sqlService.execute("CREATE JOB job AS "
                + "SINK INTO map SELECT \"int\", string FROM TABLE(JSON_FILE('" + RESOURCES_PATH + "', 'file.json'))");
        assertThat(planCache(instance()).size()).isZero();
    }

    private static void createMapping(String name, String fieldName, String glob) {
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
