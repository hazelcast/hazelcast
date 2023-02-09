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

import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnector;
import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.PostgresDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.SQLException;

import static com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnector.OPTION_DATA_LINK_REF;
import static org.junit.Assert.fail;

@Category(NightlyTest.class)
public class PostgreSinkJdbcSqlConnectorWithSchemaTest extends JdbcSqlTestSupport {

    // Quoted table name in the DB
    private String tableName = "\"table.with.dot.in.name\"";

    // Quoted schema  name in the DB
    private static final String schemaName = "\"schema.with.dot.in.name\"";

    @BeforeClass
    public static void beforeClass() {
        initialize(new PostgresDatabaseProvider());
        try {
            // Create schema for the test suite
            executeJdbc("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        } catch (SQLException e) {
            fail();
        }
    }

    protected void myCreateMapping(String mappingName) {
        // Calcite problem !
        // Currently EXTERNAL NAME can not be
        //\"\"schema.with.dot.in.name\".\"table.with.dot.in.name\"\"
        // so use backtick for quoting
        execute(
                "CREATE MAPPING \"" + mappingName + "\""
                + " EXTERNAL NAME \"`schema.with.dot.in.name`.`table.with.dot.in.name`\""
                + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + " '" + OPTION_DATA_LINK_REF + "'='" + TEST_DATABASE_REF + "'"
                + ")"
        );
    }

    @Before
    public void setUp() throws Exception {
        tableName = schemaName + "." + tableName;
    }

    @Test
    public void sinkIntoTableWithExternalSchemaName() throws Exception {
        createTable(tableName);

        String mappingName = "mapping_1";
        myCreateMapping(mappingName);

        insertItems(tableName, 2);

        execute("SINK INTO " + mappingName + " VALUES (0, 'name-10')");

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "name-10"),
                new Row(1, "name-1")
        );
    }
}