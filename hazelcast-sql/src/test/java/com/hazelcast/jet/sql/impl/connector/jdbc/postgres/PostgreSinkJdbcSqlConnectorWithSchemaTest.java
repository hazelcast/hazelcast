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

package com.hazelcast.jet.sql.impl.connector.jdbc.postgres;

import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.PostgresDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.SQLException;

import static org.junit.Assert.fail;

@Category(NightlyTest.class)
public class PostgreSinkJdbcSqlConnectorWithSchemaTest extends JdbcSqlTestSupport {

    private String tableName;

    private static final String schemaName = "test";

    @BeforeClass
    public static void beforeClass() {
        initialize(new PostgresDatabaseProvider());
        try {
            executeJdbc("CREATE SCHEMA " + schemaName);
        } catch (SQLException e) {
            fail();
        }
    }

    @Before
    public void setUp() throws Exception {
        tableName = schemaName + "." + randomTableName();
        executeJdbc("CREATE SCHEMA IF NOT EXISTS test");
    }

    @Test
    public void sinkIntoTableWithExternalSchemaName() throws Exception {
        createTable(tableName);

        String mappingName = "mapping_" + randomName();
        createMapping(tableName, mappingName);

        execute("SINK INTO " + mappingName + " VALUES (0, 'name-0')");

        assertJdbcRowsAnyOrder(tableName, new Row(0, "name-0"));
    }
}
