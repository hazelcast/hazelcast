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

package com.hazelcast.jet.sql;

import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlSortTest extends SqlTestSupport {
    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test() {
        String tableName = createTable(
                new String[]{"A", "1"},
                new String[]{"B", "2"},
                new String[]{"B", "1"},
                new String[]{"C", "1"},
                new String[]{"C", "2"}
        );

        assertRowsOrdered(
                String.format("SELECT name, distance FROM %s ORDER BY distance, name", tableName),
                asList(
                        new Row("A", 1),
                        new Row("B", 1),
                        new Row("C", 1),
                        new Row("B", 2),
                        new Row("C", 2)
                )
        );
    }

    @Test
    public void test_orderByOnNonProjectExpression() {
        String tableName = createTable(
                new String[]{"A", "1"},
                new String[]{"B", "4"},
                new String[]{"H", "3"},
                new String[]{"Z", "2"}
        );

        assertRowsOrdered(
                String.format("SELECT name, distance FROM %s ORDER BY LENGTH(name) + distance", tableName),
                asList(
                        new Row("A", 1),
                        new Row("Z", 2),
                        new Row("H", 3),
                        new Row("B", 4)
                )
        );
    }

    @Test
    public void test_nullAscending() {
        String tableName = createTable(
                new String[]{"B", null},
                new String[]{"A", null},
                new String[]{"A", "1"},
                new String[]{"B", "1"}
        );

        assertRowsOrdered(
                String.format("SELECT name, distance FROM %s ORDER BY distance ASC, name ASC", tableName),
                asList(
                        new Row("A", 1),
                        new Row("B", 1),
                        new Row("A", null),
                        new Row("B", null)
                )
        );
    }

    @Test
    public void test_nullDescending() {
        String tableName = createTable(
                new String[]{"B", null},
                new String[]{"A", null},
                new String[]{"A", "1"},
                new String[]{"B", "1"}
        );

        assertRowsOrdered(
                String.format("SELECT name, distance FROM %s ORDER BY distance DESC, name DESC", tableName),
                asList(
                        new Row("B", null),
                        new Row("A", null),
                        new Row("B", 1),
                        new Row("A", 1)
                )
        );
    }

    @Test
    public void test_nullsFirstAscending() {
        String tableName = createTable(
                new String[]{"B", null},
                new String[]{"B", "1"},
                new String[]{"A", null},
                new String[]{"A", "1"},
                new String[]{"C", "1"}
        );

        assertRowsOrdered(
                String.format("SELECT name, distance FROM %s ORDER BY distance ASC NULLS FIRST, name ASC", tableName),
                asList(
                        new Row("A", null),
                        new Row("B", null),
                        new Row("A", 1),
                        new Row("B", 1),
                        new Row("C", 1)
                )
        );
    }

    @Test
    public void test_nullsLastAscending() {
        String tableName = createTable(
                new String[]{"B", null},
                new String[]{"B", "1"},
                new String[]{"A", null},
                new String[]{"A", "1"},
                new String[]{"C", "1"}
        );

        assertRowsOrdered(
                String.format("SELECT name, distance FROM %s ORDER BY distance ASC NULLS LAST, name ASC", tableName),
                asList(
                        new Row("A", 1),
                        new Row("B", 1),
                        new Row("C", 1),
                        new Row("A", null),
                        new Row("B", null)
                )
        );
    }

    @Test
    public void test_nullsFirstDescending() {
        String tableName = createTable(
                new String[]{"B", null},
                new String[]{"B", "1"},
                new String[]{"A", null},
                new String[]{"A", "1"},
                new String[]{"C", "1"}
        );

        assertRowsOrdered(
                String.format("SELECT name, distance FROM %s ORDER BY distance DESC NULLS FIRST, name DESC", tableName),
                asList(
                        new Row("B", null),
                        new Row("A", null),
                        new Row("C", 1),
                        new Row("B", 1),
                        new Row("A", 1)
                )
        );
    }

    @Test
    public void test_nullsLastDescending() {
        String tableName = createTable(
                new String[]{"B", null},
                new String[]{"B", "1"},
                new String[]{"A", null},
                new String[]{"A", "1"},
                new String[]{"C", "1"}
        );
        assertRowsOrdered(
                String.format("SELECT name, distance FROM %s ORDER BY distance DESC NULLS LAST, name DESC", tableName),
                asList(
                        new Row("C", 1),
                        new Row("B", 1),
                        new Row("A", 1),
                        new Row("B", null),
                        new Row("A", null)
                )
        );
    }

    @Test
    public void whenOrderByOnStreamingData_thenFails() {
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM TABLE(GENERATE_STREAM(1)) ORDER BY 1"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Sorting is not supported for a streaming query");
    }

    private static String createTable(String[]... values) {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("name", "distance"),
                asList(QueryDataTypeFamily.VARCHAR, QueryDataTypeFamily.INTEGER),
                asList(values)
        );
        return name;
    }
}
