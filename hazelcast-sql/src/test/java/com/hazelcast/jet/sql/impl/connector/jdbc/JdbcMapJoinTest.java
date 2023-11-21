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

public class JdbcMapJoinTest extends JdbcSqlTestSupport {

    private static final int ITEM_COUNT = 5;

    private String tableName;

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        tableName = randomTableName();
        createTable(tableName);
        insertItems(tableName, ITEM_COUNT);

        execute(
                "CREATE MAPPING " + tableName + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );
    }


    @Test
    public void joinWithIMap() {
        String mapName = "my_map";
        execute(
                "CREATE MAPPING " + mapName + " ( " +
                "__key INT, " +
                "id INT, " +
                "name VARCHAR ) " +
                "TYPE IMap " +
                "OPTIONS (" +
                "    'keyFormat' = 'int'," +
                "    'valueFormat' = 'compact',\n" +
                "    'valueCompactTypeName' = 'person'" +
                ")"
        );

        // generate_series(1,5)
        // This way we will have
        //  - 1 item in the table (0, name-0) which does not have a corresponding item in the IMap
        // - 1 item from the IMap (5, name-5), that does not have a corresponding item in the table
        execute("INSERT INTO " + mapName + "(__key, id, name)" +
                " SELECT v,v,'name-' || v FROM TABLE(generate_series(1,5))");

        assertRowsAnyOrder(
                "SELECT t1.id, t2.name " +
                "FROM " + tableName + " t1 " +
                "JOIN " + mapName + " t2 " +
                "   ON t1.id = t2.id",
                newArrayList(
                        new Row(1, "name-1"),
                        new Row(2, "name-2"),
                        new Row(3, "name-3"),
                        new Row(4, "name-4")
                )
        );
    }
}
