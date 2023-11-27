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

import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import com.hazelcast.test.jdbc.TestDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class PredicatePushDownJdbcSqlConnectorTest extends JdbcSqlTestSupport {

    protected static String tableName;

    @Parameterized.Parameter
    public String query;

    /**
     * The test inserts 2 records (see {@link #beforeClass()} and then runs the query
     * It expects to match the first record but not the 2nd
     */
    @Parameterized.Parameters(name = "query:{0}")
    public static Object[] parameters() {
        return new Object[]{
                // Logical operators
                "SELECT name FROM people WHERE name = 'John Doe'",
                "SELECT name FROM people WHERE a = 1 AND b = 1",
                "SELECT name FROM people WHERE a = 1 OR b = 1",
                "SELECT name FROM people WHERE NOT (c = 1)",
                "SELECT name FROM people WHERE c != d",

                // Comparison operators
                "SELECT name FROM people WHERE age < 31",
                "SELECT name FROM people WHERE age < 31 AND age > 29",
                "SELECT name FROM people WHERE age <= 30",
                "SELECT name FROM people WHERE 31 > age",
                "SELECT name FROM people WHERE 30 >= age",
                "SELECT name FROM people WHERE age = 30",
                "SELECT name FROM people WHERE age != 35",
                "SELECT name FROM people WHERE age <> 35",
                "SELECT name FROM people WHERE age BETWEEN 29 AND 30",
                "SELECT name FROM people WHERE age NOT BETWEEN 34 AND 35",
                "SELECT name FROM people WHERE name LIKE 'John%'",
                "SELECT name FROM people WHERE name NOT LIKE 'Jane%'",
                // IN operator
                "SELECT name FROM people WHERE id IN (0, 1)", // PK field
                "SELECT name FROM people WHERE age IN (28, 29, 30)",
                "SELECT name FROM people WHERE name IN ('John Doe', 'something')",
                "SELECT name FROM people WHERE name NOT IN ('Jane Doe', 'something')",

                // IS Operator
                "SELECT name FROM people WHERE nullable_column IS NULL",
                "SELECT name FROM people WHERE nullable_column_reverse IS NOT NULL",
                "SELECT name FROM people WHERE a = 1 IS TRUE",
                "SELECT name FROM people WHERE c = 1 IS FALSE",
                "SELECT name FROM people WHERE c = 1 IS NOT TRUE",
                "SELECT name FROM people WHERE a = 1 IS NOT FALSE",

                // Mathematical operators
                "SELECT name FROM people WHERE age + 1 = 31",
                "SELECT name FROM people WHERE age - 1 = 29",
                "SELECT name FROM people WHERE age * 2 = 60",
                "SELECT name FROM people WHERE age / 5 = 6",

                // Conditional expressions
                "SELECT name FROM people WHERE CASE age WHEN 30 THEN TRUE ELSE FALSE END",
                "SELECT name FROM people WHERE CASE WHEN age = 30 THEN TRUE ELSE FALSE END",
                "SELECT name FROM people WHERE NULLIF(age, 30) IS NULL",
                "SELECT name FROM people WHERE COALESCE(nullable_column, nullable_column_reverse) = 'not null reverse'",

                // Conversions
                "SELECT name FROM people WHERE CAST(age AS VARCHAR(100)) = '30'",
                "SELECT name FROM people WHERE CAST(age AS VARCHAR(100)) = 'not a number' or age = 30",

                // String functions
                "SELECT name FROM people WHERE 'Hello '||name = 'Hello John Doe'",
                "SELECT name FROM people WHERE LENGTH(data) = 12",
                "SELECT name FROM people WHERE LOWER(name) = 'john doe'",
                "SELECT name FROM people WHERE UPPER(name) = 'JOHN DOE'",

                // https://docs.hazelcast.com/hazelcast/latest/sql/functions-and-operators#hide-nav
                // Mathematical Functions
                // String Functions
                // Trigonometric Functions
        };
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        initializePredicatePushDownTest(new H2DatabaseProvider());
    }

    protected static void initializePredicatePushDownTest(TestDatabaseProvider provider) throws SQLException {
        initialize(provider);

        tableName = "people";

        createTable(tableName,
                "id INT PRIMARY KEY",
                "name VARCHAR(100)",
                "age INT",
                "data VARCHAR(100)",
                "a INT", "b INT", "c INT", "d INT",
                "nullable_column VARCHAR(100)",
                "nullable_column_reverse VARCHAR(100)"
        );

        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES (1, 'John Doe', 30, '{\"value\":42}', 1, 1, 0, " +
                "1, null, 'not null reverse')");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES (2, 'Jane Doe', 35, '{\"value\":0}', 0, 0, 1, " +
                "1, 'not null', null)");
    }

    @Before
    public void setUp() throws Exception {
        // Needs to be in @Before because mappings are cleared between tests
        sqlService.execute(
                "CREATE MAPPING " + tableName + " DATA CONNECTION " + TEST_DATABASE_REF
        );
    }

    @Test
    public void runQueryExpectSingleResult_and_FullScanPhysicalRel() {
        assertRowsAnyOrder(query, new Row("John Doe"));

        assertExplain(
                "EXPLAIN " + query,
                "FullScanPhysicalRel"
        );
    }

    private void assertExplain(String query, String... rels) {
        try (SqlResult result = sqlService.execute(query)) {
            Iterator<SqlRow> it = result.iterator();
            List<String> rows = new ArrayList<>();
            while (it.hasNext()) {
                SqlRow next = it.next();
                String row = next.getObject(0);
                rows.add(row.substring(0, row.indexOf('(')));
            }
            assertThat(rows).containsExactly(rels);
            assertThat(rows).hasSameSizeAs(rels);
        }
    }

}
