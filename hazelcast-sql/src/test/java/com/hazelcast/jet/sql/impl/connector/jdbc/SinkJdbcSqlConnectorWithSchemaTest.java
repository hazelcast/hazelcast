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

import java.sql.SQLException;

import static com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnector.OPTION_DATA_LINK_REF;
import static org.junit.Assert.fail;

public class SinkJdbcSqlConnectorWithSchemaTest extends JdbcSqlTestSupport {

    String tableName = "\"table.with.dot.in.name\"";
    private static final String schemaName = "\"schema.with.dot.in.name\"";

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider(schemaName, true));
        try {
            // Create schema for the test suite
            executeJdbc("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        } catch (SQLException e) {
            fail();
        }
    }

    @Before
    public void setUp() throws Exception {
        tableName = schemaName + "." + tableName;
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
    @Test
    public void sinkIntoTableWithExternalSchemaName() throws Exception {
        createTable(tableName);

        String mappingName = "mapping_" + randomName();
        myCreateMapping(mappingName);

        execute("SINK INTO " + mappingName + " VALUES (0, 'name-0')");

        assertJdbcRowsAnyOrder(tableName, new Row(0, "name-0"));
    }
}