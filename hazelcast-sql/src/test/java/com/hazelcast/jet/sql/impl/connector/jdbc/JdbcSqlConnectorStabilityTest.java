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

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static com.hazelcast.function.ConsumerEx.noop;
import static com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnector.OPTION_DATA_LINK_NAME;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Lists.newArrayList;

public class JdbcSqlConnectorStabilityTest extends JdbcSqlTestSupport {

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
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + " '" + OPTION_DATA_LINK_NAME + "'='" + TEST_DATABASE_REF + "'"
                        + ")"
        );
    }

    @Test
    public void dataLinkDownShouldTimeout() {

        assertRowsAnyOrder(
                "SELECT * FROM " + tableName,
                newArrayList(
                        new Row(0, "name-0"),
                        new Row(1, "name-1"),
                        new Row(2, "name-2"),
                        new Row(3, "name-3"),
                        new Row(4, "name-4")
                )
        );

        databaseProvider.shutdown();

        assertThatThrownBy(() -> {
            instance()
                    .getSql()
                    .execute("SELECT * FROM " + tableName)
                    .forEach(noop());
        }).isInstanceOf(HazelcastSqlException.class);
    }

    @Test
    @Ignore("https://github.com/hazelcast/hazelcast/issues/22651")
    public void dataLinkDownShouldNotAffectUnrelatedQueries() {

        assertRowsAnyOrder(
                "SELECT * FROM " + tableName,
                newArrayList(
                        new Row(0, "name-0"),
                        new Row(1, "name-1"),
                        new Row(2, "name-2"),
                        new Row(3, "name-3"),
                        new Row(4, "name-4")
                )
        );

        databaseProvider.shutdown();

        assertRowsAnyOrder(
                "SELECT * FROM TABLE(generate_series(0, 4))",
                newArrayList(
                        new Row(0),
                        new Row(1),
                        new Row(2),
                        new Row(3),
                        new Row(4)
                )
        );
    }

}
