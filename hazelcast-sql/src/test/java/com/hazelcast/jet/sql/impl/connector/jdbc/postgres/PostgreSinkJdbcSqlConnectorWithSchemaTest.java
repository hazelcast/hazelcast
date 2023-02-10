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
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.jdbc.PostgresDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnector.OPTION_DATA_LINK_REF;
import static org.junit.Assert.fail;

@RunWith(HazelcastParametrizedRunner.class)
@Category(QuickTest.class)
public class PostgreSinkJdbcSqlConnectorWithSchemaTest extends JdbcSqlTestSupport {

    @Parameter
    public String schemaName;


    @Parameter(value = 1)
    public String tableName;

    @Parameter(value = 2)
    public String externalTableName;

    @Parameters(name = "{index}: schemaName - {0} tableName - {0}")
    public static List<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {
                        "schema1",
                        "schema1.table1",
                        "schema1.table1"
                },
                {
                        "\"schema.with.dot.in.name\"",
                        "\"schema.with.dot.in.name\".\"table.with.dot.in.name\"",
                        // Calcite problem !
                        // Currently EXTERNAL NAME can not be quoted with " character
                        // so use backtick for quoting
                        "`schema.with.dot.in.name`.`table.with.dot.in.name`"
                }
        });
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(new PostgresDatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        try {
            // Create schema for the test suite
            executeJdbc("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        } catch (SQLException e) {
            fail();
        }
    }

    protected void myCreateMapping(String mappingName) {
        execute(
                "CREATE MAPPING \"" + mappingName + "\""
                + " EXTERNAL NAME \"" + externalTableName + "\""
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
