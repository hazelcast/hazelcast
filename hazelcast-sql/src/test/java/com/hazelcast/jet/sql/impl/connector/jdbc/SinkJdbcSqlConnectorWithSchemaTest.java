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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SinkJdbcSqlConnectorWithSchemaTest extends JdbcSqlTestSupport {

    private String tableName;
    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider(true));
    }

    @Before
    public void setUp() throws Exception {
        tableName = randomTableName();
        executeJdbc("CREATE SCHEMA test");
    }
    @Test
    public void sinkIntoTableWithExternalSchemaName() throws Exception {
        tableName = "TEST.mytable";
        createTable(tableName);

        String mappingName = "mapping_" + randomName();
        createMapping(tableName, mappingName);

        execute("SINK INTO " + mappingName + " VALUES (0, 'name-0')");

        assertJdbcRowsAnyOrder(tableName, new Row(0, "name-0"));
    }

}
