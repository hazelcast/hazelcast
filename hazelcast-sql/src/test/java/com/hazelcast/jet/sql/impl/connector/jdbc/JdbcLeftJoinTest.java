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

import static org.assertj.core.util.Lists.newArrayList;

public class JdbcLeftJoinTest extends JdbcSqlTestSupport {

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


    // Left side is batch : joinInfo indices are used
    @Test
    public void joinWithOtherJdbc_right_Side_Has_Less_Rows() throws SQLException {
        String otherTableName = randomTableName();
        createTable(otherTableName);
        insertItems(otherTableName, 1, 1);
        insertItems(otherTableName, 3, 1);

        execute(
                "CREATE MAPPING " + otherTableName + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        assertRowsAnyOrder(
                "SELECT t1.id, t2.name " +
                "FROM " + tableName + " t1 " +
                "LEFT JOIN " + otherTableName + " t2 " +
                "   ON t1.id = t2.id",
                newArrayList(
                        new Row(0, null),
                        new Row(1, "name-1"),
                        new Row(2, null),
                        new Row(3, "name-3"),
                        new Row(4, null)
                        )
        );
    }

    // Left side is batch : joinInfo indices are used
    @Test
    public void leftJoinWithOtherJdbc() throws SQLException {
        String otherTableName = randomTableName();
        createTable(otherTableName);
        insertItems(otherTableName, 3);

        execute(
                "CREATE MAPPING " + otherTableName + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        assertRowsAnyOrder(
                "SELECT t1.id, t2.name " +
                "FROM " + tableName + " t1 " +
                "LEFT JOIN " + otherTableName + " t2 " +
                "   ON t1.id = t2.id",
                newArrayList(
                        new Row(0, "name-0"),
                        new Row(1, "name-1"),
                        new Row(2, "name-2"),
                        new Row(3, null),
                        new Row(4, null)
                )
        );
    }
}
