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

import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.OffsetDateTime;

import static com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector.date;
import static com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector.time;
import static com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector.timestamp;
import static com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector.timestampTz;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnitParamsRunner.class)
public class SqlTumbleTest extends SqlTestSupport {

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
    public void test_validArguments(QueryDataTypeFamily orderingColumnType, String windowSize, Object[]... values) {
        String name = randomName();
        TestStreamSqlConnector.create(
                sqlService,
                name,
                singletonList("ts"),
                singletonList(orderingColumnType),
                values
        );

        try (SqlResult result = sqlService.execute("SELECT * FROM " +
                "TABLE(TUMBLE(TABLE " + name + " , DESCRIPTOR(ts), " + windowSize + "))")
        ) {
            assertThat(result.getRowMetadata().findColumn("window_start")).isGreaterThan(-1);
            assertThat(result.getRowMetadata().getColumn(0).getType()).isEqualTo(orderingColumnType.getPublicType());
            assertThat(result.getRowMetadata().findColumn("window_end")).isGreaterThan(-1);
            assertThat(result.getRowMetadata().getColumn(1).getType()).isEqualTo(orderingColumnType.getPublicType());
            assertThat(result.iterator()).hasNext();
        }
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
    public void test_invalidArguments(QueryDataTypeFamily orderingColumnType, String windowSize) {
        String name = randomName();
        TestStreamSqlConnector.create(sqlService, name, singletonList("ts"), singletonList(orderingColumnType));

        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM " +
                "TABLE(TUMBLE(TABLE " + name + " , DESCRIPTOR(ts), " + windowSize + "))")
        ).hasMessageContaining("Cannot apply 'TUMBLE' function to [ROW, COLUMN_LIST");
    }

    @Test
    public void test_groupBy() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), null, 1),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(4), "Alice", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY 1, 2", // field ordinals
                asList(
                        new Row(timestampTz(0L), timestampTz(2L)),
                        new Row(timestampTz(2L), timestampTz(4L)),
                        new Row(timestampTz(4L), timestampTz(6L))
                )
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end, name FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.003' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.003' SECOND" +
                        ")) " +
                        "GROUP BY window_end, window_start, name",
                asList(
                        new Row(timestampTz(0L), timestampTz(3L), "Alice"),
                        new Row(timestampTz(0L), timestampTz(3L), null),
                        new Row(timestampTz(3L), timestampTz(6L), "Bob"),
                        new Row(timestampTz(3L), timestampTz(6L), "Alice")
                )
        );
    }

    @Test
    public void test_groupByNotSelectedField() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Bob", 1),
                row(timestampTz(3), "Alice", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start, name",
                asList(
                        new Row(timestampTz(0L)),
                        new Row(timestampTz(2L)),
                        new Row(timestampTz(2L))
                )
        );
    }

    @Test
    public void test_groupByExpression() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Bob", 1),
                row(timestampTz(3), "Alice", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name || '-s' AS n FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start, n",
                asList(
                        new Row(timestampTz(0L), "Alice-s"),
                        new Row(timestampTz(2L), "Bob-s"),
                        new Row(timestampTz(2L), "Alice-s")
                )
        );
    }

    @Test
    public void test_groupByHaving() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Bob", 1),
                row(timestampTz(3), "Alice", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name || '-s' AS n FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start, name " +
                        "HAVING LENGTH(n) > 5",
                asList(
                        new Row(timestampTz(0L), "Alice-s"),
                        new Row(timestampTz(2L), "Alice-s")
                )
        );
    }

    @Test
    public void test_groupByEmpty() {
        String name = createTable();

        assertEmptyResult("SELECT window_start FROM " +
                "TABLE(TUMBLE(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                "  , DESCRIPTOR(ts)" +
                "  , INTERVAL '0.002' SECOND" +
                ")) " +
                "GROUP BY window_start"
        );
    }

    @Test
    public void test_count() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), null, null),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, COUNT(name) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(0L), 1L),
                        new Row(timestampTz(2L), 2L)
                )
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, COUNT(*) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(0L), 2L),
                        new Row(timestampTz(2L), 2L)
                )
        );
    }

    @Test
    public void test_countDistinct() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Bob", 1),
                row(timestampTz(3), "Alice", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, COUNT(DISTINCT name) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(0L), 1L),
                        new Row(timestampTz(2L), 2L)
                )
        );
    }

    @Test
    public void test_countGroupedBy() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 2),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(10), null, null)

        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, COUNT(distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(0L), "Alice", 3L),
                        new Row(timestampTz(2L), "Alice", 1L),
                        new Row(timestampTz(2L), "Bob", 1L)
                )
        );
    }

    @Test
    public void test_countDistinctGroupedBy() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 2),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, COUNT(DISTINCT distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(0L), "Alice", 2L),
                        new Row(timestampTz(2L), "Alice", 1L),
                        new Row(timestampTz(2L), "Bob", 1L)
                )
        );
    }

    @Test
    public void test_countGroupedByHaving() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(2), "Bob", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(4), "Joey", 1),
                row(timestampTz(5), "Joey", 1),
                row(timestampTz(5), "Joey", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, COUNT(*) c FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start HAVING c <> 2",
                asList(
                        new Row(timestampTz(0L), "Alice", 1L),
                        new Row(timestampTz(4L), "Joey", 3L)
                )
        );
    }

    @Test
    public void test_countFilter() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Bob", 1),
                row(timestampTz(3), "Alice", 1),
                row(timestampTz(5), "Joey", 1),
                row(timestampTz(5), "Joey", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, COUNT(name) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "WHERE ts > '" + timestampTz(0) + "' " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(0L), "Bob", 1L),
                        new Row(timestampTz(2L), "Alice", 1L),
                        new Row(timestampTz(4L), "Joey", 2L)
                )
        );
    }

    @Test
    public void test_countEmpty() {
        String name = createTable();

        assertEmptyResult(
                "SELECT window_start, COUNT(*) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start"
        );
    }

    @Test
    public void test_min() {
        String name = createTable(
                row(timestampTz(0), "Alice", 2),
                row(timestampTz(0), "Bob", 1),
                row(timestampTz(1), null, null),
                row(timestampTz(2), "Bob", 2),
                row(timestampTz(3), "Joey", 3),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, MIN(name), MIN(distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(0L), "Alice", 1),
                        new Row(timestampTz(2L), "Bob", 2)
                )
        );
    }

    @Test
    public void test_minDistinct() {
        String name = createTable(
                row(timestampTz(0), "Bob", 1),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Bob", 1),
                row(timestampTz(3), "Joey", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, MIN(DISTINCT name) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(0L), "Alice"),
                        new Row(timestampTz(2L), "Bob")
                )
        );
    }

    @Test
    public void test_minGroupedBy() {
        String name = createTable(
                row(timestampTz(0), "Alice", 2),
                row(timestampTz(1), "Bob", 2),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Alice", 2),
                row(timestampTz(2), "Bob", 1),
                row(timestampTz(3), "Bob", 2),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, MIN(distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(0L), "Alice", 1),
                        new Row(timestampTz(0L), "Bob", 2),
                        new Row(timestampTz(2L), "Alice", 2),
                        new Row(timestampTz(2L), "Bob", 1)
                )
        );
    }

    @Test
    public void test_minGroupedByHaving() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(2), "Bob", 2),
                row(timestampTz(3), "Bob", 3),
                row(timestampTz(4), "Alice", 3),
                row(timestampTz(4), "Alice", 4),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, MIN(distance) m FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start HAVING m > 1",
                asList(
                        new Row(timestampTz(2L), "Bob", 2),
                        new Row(timestampTz(4L), "Alice", 3)
                )
        );
    }

    @Test
    public void test_minFilter() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Bob", 2),
                row(timestampTz(1), "Bob", 1),
                row(timestampTz(3), "Alice", 3),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, MIN(distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "WHERE ts > '" + timestampTz(0) + "' " +
                        "GROUP BY window_start, name",
                asList(
                        new Row(timestampTz(0L), "Bob", 1),
                        new Row(timestampTz(2L), "Alice", 3)
                )
        );
    }

    @Test
    public void test_minEmpty() {
        String name = createTable();

        assertEmptyResult(
                "SELECT window_start, MIN(name) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start"
        );
    }

    @Test
    public void test_max() {
        String name = createTable(
                row(timestampTz(0), "Alice", 2),
                row(timestampTz(0), "Bob", 1),
                row(timestampTz(1), null, null),
                row(timestampTz(2), "Bob", 2),
                row(timestampTz(3), "Joey", 3),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, MAX(name), MAX(distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(0L), "Bob", 2),
                        new Row(timestampTz(2L), "Joey", 3)
                )
        );
    }

    @Test
    public void test_maxDistinct() {
        String name = createTable(
                row(timestampTz(0), "Bob", 1),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Bob", 1),
                row(timestampTz(3), "Joey", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, MAX(DISTINCT name) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(0L), "Bob"),
                        new Row(timestampTz(2L), "Joey")
                )
        );
    }

    @Test
    public void test_maxGroupedBy() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Bob", 1),
                row(timestampTz(1), "Alice", 2),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(2), "Bob", 2),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, MAX(distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(0L), "Alice", 2),
                        new Row(timestampTz(0L), "Bob", 1),
                        new Row(timestampTz(2L), "Alice", 1),
                        new Row(timestampTz(2L), "Bob", 2)
                )
        );
    }

    @Test
    public void test_maxGroupedByHaving() {
        String name = createTable(
                row(timestampTz(0), "Alice", 3),
                row(timestampTz(2), "Bob", 2),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(4), "Alice", 1),
                row(timestampTz(4), "Alice", 0),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, MAX(distance) m FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start HAVING m < 3",
                asList(
                        new Row(timestampTz(2L), "Bob", 2),
                        new Row(timestampTz(4L), "Alice", 1)
                )
        );
    }

    @Test
    public void test_maxFilter() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Bob", 2),
                row(timestampTz(1), "Bob", 1),
                row(timestampTz(3), "Alice", 3),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, MAX(distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "WHERE ts > '" + timestampTz(0) + "' " +
                        "GROUP BY window_start, name",
                asList(
                        new Row(timestampTz(0L), "Bob", 2),
                        new Row(timestampTz(2L), "Alice", 3)
                )
        );
    }

    @Test
    public void test_maxEmpty() {
        String name = createTable();

        assertEmptyResult(
                "SELECT window_start, MAX(name) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start"
        );
    }

    @Test
    public void test_sum() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), null, null),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, SUM(distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(0L), 1L),
                        new Row(timestampTz(2L), 2L)
                )
        );
    }

    @Test
    public void test_sumDistinct() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Bob", 1),
                row(timestampTz(3), "Alice", 2),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, SUM(DISTINCT distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(0L), 1L),
                        new Row(timestampTz(2L), 3L)
                )
        );
    }

    @Test
    public void test_sumGroupedBy() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 2),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(3), "Bob", 2),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, SUM(distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(0L), "Alice", 4L),
                        new Row(timestampTz(2L), "Alice", 1L),
                        new Row(timestampTz(2L), "Bob", 3L)
                )
        );
    }

    @Test
    public void test_sumDistinctGroupedBy() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 2),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(3), "Bob", 2),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, SUM(DISTINCT distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(0L), "Alice", 3L),
                        new Row(timestampTz(2L), "Alice", 1L),
                        new Row(timestampTz(2L), "Bob", 3L)
                )
        );
    }

    @Test
    public void test_sumGroupedByHaving() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(2), "Bob", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(4), "Joey", 1),
                row(timestampTz(5), "Joey", 1),
                row(timestampTz(5), "Joey", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, SUM(distance) s FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start HAVING s > 1",
                asList(
                        new Row(timestampTz(2L), "Bob", 2L),
                        new Row(timestampTz(4L), "Joey", 3L)
                )
        );
    }

    @Test
    public void test_sumFilter() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Bob", 1),
                row(timestampTz(3), "Alice", 1),
                row(timestampTz(5), "Joey", 1),
                row(timestampTz(5), "Joey", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, SUM(distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "WHERE ts > '" + timestampTz(0) + "' " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(0L), "Bob", 1L),
                        new Row(timestampTz(2L), "Alice", 1L),
                        new Row(timestampTz(4L), "Joey", 2L)
                )
        );
    }

    @Test
    public void test_sumEmpty() {
        String name = createTable();

        assertEmptyResult(
                "SELECT window_start, SUM(distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start"
        );
    }

    @Test
    public void test_avg() {
        String name = createTable(
                row(timestampTz(0), "Alice", 2),
                row(timestampTz(1), null, null),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, AVG(distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(0L), new BigDecimal(2)),
                        new Row(timestampTz(2L), new BigDecimal(1))
                )
        );
    }

    @Test
    public void test_avgDistinct() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Bob", 1),
                row(timestampTz(3), "Alice", 2),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, AVG(DISTINCT distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(0L), new BigDecimal(1)),
                        new Row(timestampTz(2L), new BigDecimal("1.5"))
                )
        );
    }

    @Test
    public void test_avgGroupedBy() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 4),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(3), "Bob", 2),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, AVG(distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(0L), "Alice", new BigDecimal(2)),
                        new Row(timestampTz(2L), "Alice", new BigDecimal(1)),
                        new Row(timestampTz(2L), "Bob", new BigDecimal("1.5"))
                )
        );
    }

    @Test
    public void test_avgDistinctGroupedBy() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 2),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(3), "Bob", 2),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, AVG(DISTINCT distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(0L), "Alice", new BigDecimal("1.5")),
                        new Row(timestampTz(2L), "Alice", new BigDecimal(1)),
                        new Row(timestampTz(2L), "Bob", new BigDecimal("1.5"))
                )
        );
    }

    @Test
    public void test_avgGroupedByHaving() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Bob", 1),
                row(timestampTz(3), "Bob", 3),
                row(timestampTz(4), "Joey", 1),
                row(timestampTz(5), "Joey", 4),
                row(timestampTz(5), "Joey", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, AVG(distance) a FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start HAVING a > 1",
                asList(
                        new Row(timestampTz(2L), "Bob", new BigDecimal(2)),
                        new Row(timestampTz(4L), "Joey", new BigDecimal(2))
                )
        );
    }

    @Test
    public void test_avgFilter() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Bob", 1),
                row(timestampTz(3), "Alice", 1),
                row(timestampTz(5), "Joey", 3),
                row(timestampTz(5), "Joey", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, AVG(distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "WHERE ts > '" + timestampTz(0) + "' " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(0L), "Bob", new BigDecimal(1)),
                        new Row(timestampTz(2L), "Alice", new BigDecimal(1)),
                        new Row(timestampTz(4L), "Joey", new BigDecimal(2))
                )
        );
    }

    @Test
    public void test_avgEmpty() {
        String name = createTable();

        assertEmptyResult(
                "SELECT window_start, AVG(distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start"
        );
    }

    @Test
    public void test_multipleAggregations() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Bob", 1),
                row(timestampTz(3), "Alice", 1),
                row(timestampTz(5), "Bob", 3),
                row(timestampTz(5), "Joey", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, COUNT(*), MIN(name), MAX(name), SUM(distance), AVG(distance) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(0L), 2L, "Alice", "Bob", 2L, new BigDecimal(1)),
                        new Row(timestampTz(2L), 1L, "Alice", "Alice", 1L, new BigDecimal(1)),
                        new Row(timestampTz(4L), 2L, "Bob", "Joey", 4L, new BigDecimal(2))
                )
        );
    }

    @SuppressWarnings("unused")
    private Object[] orderingColumnTypes() {
        return new Object[]{
                new Object[]{
                        TINYINT,
                        "2",
                        row((byte) 0), row((byte) 4)
                },
                new Object[]{
                        SMALLINT,
                        "2",
                        row((short) 0), row((short) 4)
                },
                new Object[]{
                        INTEGER,
                        "2",
                        row(0), row(4)
                },
                new Object[]{
                        BIGINT,
                        "2",
                        row(0L), row(4L)
                },
                new Object[]{
                        TIME,
                        "INTERVAL '0.002' SECOND",
                        row(time(0)), row(time(4))
                },
                new Object[]{
                        DATE,
                        "INTERVAL '2' DAYS",
                        row(date(0)), row(date(345_600_000))
                },
                new Object[]{
                        TIMESTAMP,
                        "INTERVAL '0.002' SECOND",
                        row(timestamp(0)), row(timestamp(4))
                },
                new Object[]{
                        TIMESTAMP_WITH_TIME_ZONE,
                        "INTERVAL '0.002' SECOND",
                        row(timestampTz(0)), row(timestampTz(4))
                },
        };
    }

    @Test
    @Parameters(method = "orderingColumnTypes")
    public void test_orderingColumnTypes(QueryDataTypeFamily orderingColumnType, String windowSize, Object[]... values) {
        String name = randomName();
        TestStreamSqlConnector.create(
                sqlService,
                name,
                singletonList("ts"),
                singletonList(orderingColumnType),
                values
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT COUNT(*) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), " + windowSize + ")))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , " + windowSize +
                        ")) " +
                        "GROUP BY window_start",
                singletonList(new Row(1L))
        );
    }


    @Test
    public void test_nested_filter() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start_inner, name, COUNT(name) FROM " +
                        "TABLE(TUMBLE(" +
                        "   (SELECT ts, name, window_start window_start_inner FROM" +
                        "      TABLE(TUMBLE(" +
                        "           (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "           , DESCRIPTOR(ts)" +
                        "           , INTERVAL '0.002' SECOND" +
                        "       )) WHERE ts > '" + timestampTz(0) + "' " +
                        "   )" +
                        "   , DESCRIPTOR(ts)" +
                        "   , INTERVAL '0.003' SECOND" +
                        ")) " +
                        "GROUP BY window_start_inner, name",
                asList(
                        new Row(timestampTz(0L), "Alice", 1L),
                        new Row(timestampTz(2L), "Alice", 1L),
                        new Row(timestampTz(2L), "Bob", 1L)
                )
        );
    }

    @Test
    public void test_nested_project() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start_inner_2, window_start_inner_1, name, COUNT(name) FROM " +
                        "TABLE(TUMBLE(" +
                        "   (SELECT ts, name, window_start window_start_inner_1, window_start window_start_inner_2 FROM" +
                        "      TABLE(TUMBLE(" +
                        "           (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "           , DESCRIPTOR(ts)" +
                        "           , INTERVAL '0.002' SECOND" +
                        "       ))" +
                        "   )" +
                        "   , DESCRIPTOR(ts)" +
                        "   , INTERVAL '0.003' SECOND" +
                        ")) " +
                        "GROUP BY window_start_inner_2, window_start_inner_1, name",
                asList(
                        new Row(timestampTz(0L), timestampTz(0L), "Alice", 2L),
                        new Row(timestampTz(2L), timestampTz(2L), "Alice", 1L),
                        new Row(timestampTz(2L), timestampTz(2L), "Bob", 1L)
                )
        );
    }

    @Test
    public void test_nested_aggregate() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Bob", 1),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Alice", 1),
                row(timestampTz(5), "Bob", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start_inner, name, COUNT(*) FROM " +
                        "TABLE(TUMBLE(" +
                        "   (SELECT name, window_start window_start_inner FROM " +
                        "       TABLE(TUMBLE(" +
                        "           (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "           , DESCRIPTOR(ts)" +
                        "           , INTERVAL '0.002' SECOND" +
                        "       )) GROUP BY name, window_start_inner" +
                        "   )" +
                        "   , DESCRIPTOR(window_start_inner)" +
                        "   , INTERVAL '0.003' SECOND" +
                        ")) " +
                        "GROUP BY window_start_inner, name",
                asList(
                        new Row(timestampTz(0L), "Alice", 1L),
                        new Row(timestampTz(0L), "Bob", 1L),
                        new Row(timestampTz(2L), "Alice", 1L),
                        new Row(timestampTz(4L), "Bob", 1L)
                )
        );
    }

    @Test
    public void test_nested_join() {
        createMapping("map", OffsetDateTime.class, String.class);
        instance().getMap("map").put(timestampTz(0), "value-0");
        instance().getMap("map").put(timestampTz(1), "value-1");
        instance().getMap("map").put(timestampTz(2), "value-1");
        instance().getMap("map").put(timestampTz(3), "value-1");

        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Bob", 1),
                row(timestampTz(2), "Joey", 1),
                row(timestampTz(3), "Alice", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start_inner, this, COUNT(*) FROM " +
                        "TABLE(TUMBLE(" +
                        "   (SELECT ts, window_start window_start_inner, this FROM " +
                        "       TABLE(TUMBLE(" +
                        "           (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "           , DESCRIPTOR(ts)" +
                        "           , INTERVAL '0.002' SECOND" +
                        "       )) JOIN map ON ts = __key" +
                        "   )" +
                        "   , DESCRIPTOR(ts)" +
                        "   , INTERVAL '0.003' SECOND" +
                        ")) " +
                        "GROUP BY window_start_inner, this",
                asList(
                        new Row(timestampTz(0L), "value-0", 1L),
                        new Row(timestampTz(0L), "value-1", 1L),
                        new Row(timestampTz(2L), "value-1", 2L)
                )
        );
    }

    @Test
    public void test_namedParameters() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end, COUNT(name) FROM " +
                        "TABLE(TUMBLE(" +
                        "   window_size => INTERVAL '0.002' SECOND" +
                        "   , \"column\" => DESCRIPTOR(ts)" +
                        "   , input => (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        ")) " +
                        "GROUP BY window_start, window_end",
                asList(
                        new Row(timestampTz(0L), timestampTz(2L), 1L),
                        new Row(timestampTz(2L), timestampTz(4L), 2L)
                )
        );
    }

    @Test
    @Ignore("flaky") // TODO:
    public void test_dropLateRows() {
        String name = createTable(
                row(timestampTz(3), "Alice", 1),
                row(timestampTz(2), "Bob", 1),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(4), "Alice", 1),
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(2), "Bob", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, COUNT(*) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(0L), 1L),
                        new Row(timestampTz(2L), 3L),
                        new Row(timestampTz(4L), 1L)
                )
        );
    }

    @Test
    public void when_windowEdgeIsProjected_then_regularAggregationIsApplied() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), null, 1),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(10), null, null)
        );

        assertEmptyResult(
                "SELECT window_start + INTERVAL '0.001' SECOND, COUNT(name) FROM " +
                        "TABLE(TUMBLE(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start + INTERVAL '0.001' SECOND"
        );
    }

    @Test
    public void test_batchSource() {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name"),
                asList(TIMESTAMP_WITH_TIME_ZONE, VARCHAR),
                row(timestampTz(0), "Alice"),
                row(timestampTz(1), null),
                row(timestampTz(2), "Alice"),
                row(timestampTz(3), "Bob")
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end, COUNT(name) FROM " +
                        "TABLE(TUMBLE(" +
                        "  TABLE " + name +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start, window_end",
                asList(
                        new Row(timestampTz(0L), timestampTz(2L), 1L),
                        new Row(timestampTz(2L), timestampTz(4L), 2L)
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
                asList("ts", "name", "distance"),
                asList(TIMESTAMP_WITH_TIME_ZONE, VARCHAR, INTEGER),
                values
        );
        return name;
    }
}
