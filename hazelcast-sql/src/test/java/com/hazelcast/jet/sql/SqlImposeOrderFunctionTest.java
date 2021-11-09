/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql;

import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DATE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DOUBLE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.REAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIME;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TINYINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnitParamsRunner.class)
public class SqlImposeOrderFunctionTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() throws IOException {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @SuppressWarnings("unused")
    private Object[] validArguments() {
        return new Object[]{
                new Object[]{
                        TINYINT,
                        "1",
                        row((byte) 0), row((byte) 2)
                },
                new Object[]{
                        SMALLINT,
                        "2",
                        row((short) 0), row((short) 2)
                },
                new Object[]{
                        INTEGER,
                        "3",
                        row(0), row(2)
                },
                new Object[]{
                        BIGINT,
                        "4",
                        row(0L), row(2L)
                },
                new Object[]{
                        TIME,
                        "INTERVAL '0.005' SECOND",
                        row(time(0)), row(time(2))
                },
                new Object[]{
                        DATE,
                        "INTERVAL '0.006' SECOND",
                        row(date(0)), row(date(2))
                },
                new Object[]{
                        TIMESTAMP,
                        "INTERVAL '0.007' SECOND",
                        row(timestamp(0)), row(timestamp(2))
                },
                new Object[]{
                        TIMESTAMP_WITH_TIME_ZONE,
                        "INTERVAL '0.008' SECOND",
                        row(timestampTz(0)), row(timestampTz(2))
                },
        };
    }

    @Test
    @Parameters(method = "validArguments")
    public void test_validArguments(QueryDataTypeFamily orderingColumnType, String maxLag, Object[]... values) {
        String name = randomName();
        TestStreamSqlConnector.create(sqlService, name, singletonList("ts"), singletonList(orderingColumnType), values);

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " +
                        "TABLE(IMPOSE_ORDER(TABLE(" + name + "), DESCRIPTOR(ts), " + maxLag + "))",
                Arrays.stream(values).map(Row::new).collect(toList())
        );
    }

    @SuppressWarnings("unused")
    private Object[] invalidArguments() {
        return new Object[]{
                new Object[]{TINYINT, "INTERVAL '0.001' SECOND"},
                new Object[]{SMALLINT, "INTERVAL '0.002' SECOND"},
                new Object[]{INTEGER, "INTERVAL '0.003' SECOND"},
                new Object[]{BIGINT, "INTERVAL '0.004' SECOND"},
                new Object[]{DECIMAL, "INTERVAL '0.005' SECOND"},
                new Object[]{DECIMAL, "6"},
                new Object[]{REAL, "INTERVAL '0.007' SECOND"},
                new Object[]{REAL, "8"},
                new Object[]{DOUBLE, "INTERVAL '0.009' SECOND"},
                new Object[]{DOUBLE, "10"},
                new Object[]{TIME, "11"},
                new Object[]{DATE, "12"},
                new Object[]{TIMESTAMP, "13"},
                new Object[]{TIMESTAMP_WITH_TIME_ZONE, "14"},
        };
    }

    @Test
    @Parameters(method = "invalidArguments")
    public void test_invalidArguments(QueryDataTypeFamily orderingColumnType, String maxLag) {
        String name = randomName();
        TestStreamSqlConnector.create(sqlService, name, singletonList("ts"), singletonList(orderingColumnType));

        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM " +
                "TABLE(IMPOSE_ORDER(TABLE(" + name + "), DESCRIPTOR(ts), " + maxLag + "))")
        ).hasMessageContaining("Cannot apply 'IMPOSE_ORDER' function to [ROW, COLUMN_LIST");
    }

    @Test
    public void test_layeredInvocations() {
        String name = createTable();

        assertThatThrownBy(() -> sqlService.execute(
                "SELECT * FROM " +
                        "TABLE(IMPOSE_ORDER(" +
                        "  (SELECT * FROM" +
                        "    TABLE(IMPOSE_ORDER(" +
                        "      TABLE(" + name + ")" +
                        "      , DESCRIPTOR(ts)" +
                        "      , INTERVAL '0.001' SECOND" +
                        "    ))" +
                        "  )" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        "))"
        )).hasMessageContaining("Multiple ordering functions are not supported");
    }

    @Test
    public void test_singleColumn() {
        String name = createTable();

        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM " +
                "TABLE(IMPOSE_ORDER(TABLE(" + name + "), DESCRIPTOR(), INTERVAL '0.001' SECOND))")
        ).hasMessageContaining("You must specify single ordering column");
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM " +
                "TABLE(IMPOSE_ORDER(TABLE(" + name + "), DESCRIPTOR(ts, ts), INTERVAL '0.001' SECOND))")
        ).hasMessageContaining("You must specify single ordering column");
    }

    @Test
    public void test_filteredInput() {
        String name = createTable(
                row(timestampTz(0), "Alice"),
                row(timestampTz(1), null),
                row(timestampTz(2), "Bob")
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " +
                        "TABLE(IMPOSE_ORDER(" +
                        "  (SELECT * FROM " + name + " WHERE name != 'Alice')" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.001' SECOND" +
                        "))",
                singletonList(new Row(timestampTz(2), "Bob"))
        );
    }

    @Test
    public void test_projectedInput() {
        String name = createTable(
                row(timestampTz(0), "Alice"),
                row(timestampTz(1), null),
                row(timestampTz(2), "Bob")
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " +
                        "TABLE(IMPOSE_ORDER(" +
                        "  (SELECT ts FROM " + name + ")" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.001' SECOND" +
                        "))",
                asList(
                        new Row(timestampTz(0)),
                        new Row(timestampTz(1)),
                        new Row(timestampTz(2))
                )
        );
    }

    @Test
    public void test_filteredAndProjectedInput() {
        String name = createTable(
                row(timestampTz(0), "Alice"),
                row(timestampTz(1), null),
                row(timestampTz(2), "Bob")
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " +
                        "TABLE(IMPOSE_ORDER(" +
                        "  (SELECT ts FROM " + name + " WHERE name != 'Alice')" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.001' SECOND" +
                        "))",
                singletonList(new Row(timestampTz(2)))
        );
    }

    @Test
    public void test_projectionThatCannotBePushedDown() {
        String name = createTable();

        assertThatThrownBy(() -> sqlService.execute(
                "SELECT * FROM " +
                        "TABLE(IMPOSE_ORDER(" +
                        "  (SELECT ts + INTERVAL '0.001' SECOND AS ts, name  FROM " + name + ")" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.001' SECOND" +
                        "))"
        )).hasMessageContaining("Ordering function cannot be applied to input table");
    }

    @Test
    public void test_namedParameters() {
        String name = createTable(
                row(timestampTz(0), "Alice"),
                row(timestampTz(1), null),
                row(timestampTz(2), "Bob")
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " +
                        "TABLE(IMPOSE_ORDER(" +
                        "  \"lag\" => INTERVAL '0.001' SECOND" +
                        "  , input => (TABLE(" + name + "))" +
                        "  , timeCol => DESCRIPTOR(ts)" +
                        "))",
                asList(
                        new Row(timestampTz(0), "Alice"),
                        new Row(timestampTz(1), null),
                        new Row(timestampTz(2), "Bob")
                )
        );
    }

    private static Object[] row(Object... values) {
        return values;
    }

    private static String createTable(Object[]... values) {
        String name = randomName();
        TestStreamSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name"),
                asList(TIMESTAMP_WITH_TIME_ZONE, VARCHAR),
                values
        );
        return name;
    }
}
