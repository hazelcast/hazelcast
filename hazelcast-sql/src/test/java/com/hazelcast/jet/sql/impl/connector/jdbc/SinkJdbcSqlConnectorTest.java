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

import static org.assertj.core.util.Lists.newArrayList;

public class SinkJdbcSqlConnectorTest extends JdbcSqlTestSupport {

    private String tableName;

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        tableName = randomTableName();
    }

    @Test
    public void sinkIntoTable() throws Exception {
        createTable(tableName);
        createMapping(tableName);

        execute("SINK INTO " + tableName + " VALUES (0, 'name-0')");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "name-0"));
    }

    @Test
    public void sinkIntoTableWithExternalName() throws Exception {
        createTable(tableName);
        String mappingName = "mapping_" + randomName();
        createMapping(tableName, mappingName);

        execute("SINK INTO " + mappingName + " VALUES (0, 'name-0')");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "name-0"));
    }

    @Test
    public void sinkIntoTableColumnHasExternalName() throws Exception {
        createTable(tableName);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " fullName VARCHAR EXTERNAL NAME name"
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("SINK INTO " + tableName + " VALUES (0, 'name-0')");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "name-0"));
    }

    @Test
    public void sinkIntoTableWithColumns() throws Exception {
        createTable(tableName);
        createMapping(tableName);

        execute("SINK INTO " + tableName + " (name, id) VALUES ('name-0', 0), ('name-1', 1)");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "name-0"),
                new Row(1, "name-1")
        );
    }

    @Test
    public void sinkIntoTableWithColumnsColumnHasExternalName() throws Exception {
        createTable(tableName);
        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " fullName VARCHAR EXTERNAL NAME name"
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        execute("SINK INTO " + tableName + " (fullName, id) VALUES ('name-0', 0), ('name-1', 1)");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "name-0"),
                new Row(1, "name-1")
        );
    }

    @Test
    public void sinkIntoTableMultipleValues() throws Exception {
        createTable(tableName);
        createMapping(tableName);

        execute("SINK INTO " + tableName + " SELECT v,'name-' || v FROM TABLE(generate_series(0,4))");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "name-0"),
                new Row(1, "name-1"),
                new Row(2, "name-2"),
                new Row(3, "name-3"),
                new Row(4, "name-4")
        );
    }


    @Test
    public void sinkIntoTableReverseColumnOrder() throws Exception {
        createTable(tableName, "id INT PRIMARY KEY", "name VARCHAR(10)");
        execute("CREATE MAPPING " + tableName + " DATA CONNECTION " + TEST_DATABASE_REF);

        execute("SINK INTO " + tableName + " (name, id) VALUES ('name-0', 0)");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "name-0")
        );
    }

    @Test
    public void updateTableWithColumns() throws Exception {
        createTable(tableName);
        createMapping(tableName);

        // Insert items with JDBC to make sure DB is populated without using Jet
        insertItems(tableName, 2);

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "name-0"),
                new Row(1, "name-1")
        );

        execute("SINK INTO " + tableName + " (name, id) VALUES ('name-2', 0), ('name-3', 1)");

        assertJdbcRowsAnyOrder(tableName,
                newArrayList(Integer.class, String.class),
                new Row(0, "name-2"),
                new Row(1, "name-3")
        );
    }
}
