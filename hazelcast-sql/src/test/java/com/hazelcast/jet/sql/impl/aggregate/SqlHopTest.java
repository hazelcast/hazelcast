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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.OffsetDateTime;

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

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlHopTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() throws IOException {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_validArguments_tinyInt() {
        checkValidArguments(TINYINT, "1", row((byte) 0), row((byte) 2));
    }

    @Test
    public void test_validArguments_smallInt() {
        checkValidArguments(SMALLINT, "2", row((short) 0), row((short) 2));
    }

    @Test
    public void test_validArguments_int() {
        checkValidArguments(INTEGER, "3", row(0), row(2));
    }

    @Test
    public void test_validArguments_bigInt() {
        checkValidArguments(BIGINT, "4", row(0L), row(2L));
    }

    @Test
    public void test_validArguments_time() {
        checkValidArguments(TIME, "INTERVAL '0.005' SECOND", row(time(0)), row(time(2)));
    }

    @Test
    public void test_validArguments_date() {
        checkValidArguments(DATE, "INTERVAL '6' DAYS", row(date(0)), row(date(2)));
    }

    @Test
    public void test_validArguments_timestamp() {
        checkValidArguments(TIMESTAMP, "INTERVAL '0.007' SECOND", row(timestamp(0)), row(timestamp(2)));
    }

    @Test
    public void test_validArguments_timestampTz() {
        checkValidArguments(TIMESTAMP_WITH_TIME_ZONE, "INTERVAL '0.008' SECOND", row(timestampTz(0)), row(timestampTz(2)));
    }

    private static void checkValidArguments(QueryDataTypeFamily orderingColumnType, String size, Object[]... values) {
        String name = randomName();
        TestStreamSqlConnector.create(sqlService, name, singletonList("ts"), singletonList(orderingColumnType), values);

        try (SqlResult result = sqlService.execute("SELECT * FROM " +
                "TABLE(HOP(TABLE " + name + ", DESCRIPTOR(ts), " + size + ", " + size + "))")
        ) {
            assertThat(result.getRowMetadata().findColumn("window_start")).isEqualTo(1);
            assertThat(result.getRowMetadata().getColumn(1).getType()).isEqualTo(orderingColumnType.getPublicType());
            assertThat(result.getRowMetadata().findColumn("window_end")).isEqualTo(2);
            assertThat(result.getRowMetadata().getColumn(2).getType()).isEqualTo(orderingColumnType.getPublicType());
            assertThat(result.iterator()).hasNext();
        }
    }

    @Test
    public void test_invalidArguments_tinyInt() {
        checkInvalidArguments(TINYINT, "INTERVAL '0.001' SECOND");
    }

    @Test
    public void test_invalidArguments_smallInt() {
        checkInvalidArguments(SMALLINT, "INTERVAL '0.002' SECOND");
    }

    @Test
    public void test_invalidArguments_int() {
        checkInvalidArguments(INTEGER, "INTERVAL '0.003' SECOND");
    }

    @Test
    public void test_invalidArguments_bigInt() {
        checkInvalidArguments(BIGINT, "INTERVAL '0.004' SECOND");
    }

    @Test
    public void test_invalidArguments_decimal_interval() {
        checkInvalidArguments(DECIMAL, "INTERVAL '0.005' SECOND");
    }

    @Test
    public void test_invalidArguments_decimal_number() {
        checkInvalidArguments(DECIMAL, "6");
    }

    @Test
    public void test_invalidArguments_real_interval() {
        checkInvalidArguments(REAL, "INTERVAL '0.007' SECOND");
    }

    @Test
    public void test_invalidArguments_real_number() {
        checkInvalidArguments(REAL, "8");
    }

    @Test
    public void test_invalidArguments_double_interval() {
        checkInvalidArguments(DOUBLE, "INTERVAL '0.009' SECOND");
    }

    @Test
    public void test_invalidArguments_double_number() {
        checkInvalidArguments(DOUBLE, "10");
    }

    @Test
    public void test_invalidArguments_time() {
        checkInvalidArguments(TIME, "11");
    }

    @Test
    public void test_invalidArguments_date() {
        checkInvalidArguments(DATE, "12");
    }

    @Test
    public void test_invalidArguments_timestamp() {
        checkInvalidArguments(TIMESTAMP, "13");
    }

    @Test
    public void test_invalidArguments_timestampTz() {
        checkInvalidArguments(TIMESTAMP_WITH_TIME_ZONE, "14");
    }

    private static void checkInvalidArguments(QueryDataTypeFamily orderingColumnType, String size) {
        String name = randomName();
        TestStreamSqlConnector.create(sqlService, name, singletonList("ts"), singletonList(orderingColumnType));

        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM " +
                "TABLE(HOP(TABLE " + name + ", DESCRIPTOR(ts), " + size + ", " + size + "))")
        ).hasMessageContaining("The descriptor column type")
                .hasMessageContaining("and the interval type")
                .hasMessageContaining("do not match");
    }

    @Test
    public void test_windowBounds() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(2), "Bob", 1)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end, name FROM " +
                        "TABLE(HOP(" +
                        "  TABLE " + name +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        "))",
                asList(
                        new Row(timestampTz(-2L), timestampTz(2L), "Alice"),
                        new Row(timestampTz(0L), timestampTz(4L), "Alice"),
                        new Row(timestampTz(0L), timestampTz(4L), "Bob"),
                        new Row(timestampTz(2L), timestampTz(6L), "Bob")
                )
        );
    }

    @Test
    public void test_windowBoundsWithNullDescriptorEvent() {
        String name = createTable(
                row(null, "Alice", 1),
                row(timestampTz(2), "Bob", 1)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end, name FROM " +
                        "TABLE(HOP(" +
                        "  TABLE " + name +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        "))",
                asList(
                        new Row(null, null, "Alice"),
                        new Row(timestampTz(0L), timestampTz(4L), "Bob"),
                        new Row(timestampTz(2L), timestampTz(6L), "Bob")
                )
        );
    }

    @Test
    public void test_projectWindowBounds() {
        String name = createTable(
                row(timestampTz(1000L), "Alice", 1),
                row(timestampTz(2000L), null, null)
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT SIN( CAST( EXTRACT(SECOND FROM window_start) AS DOUBLE)), SUM(distance) FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                rows(2,
                        Math.sin(0), 1L,
                        Math.sin(1), 1L));
    }

    @Test
    public void test_groupBy() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), null, 1),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(4), "Alice", 1),
                row(timestampTz(20), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start/*, window_end*/ FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY 1/*, 2*/", // field ordinals
                asList(
                        new Row(timestampTz(-2L)),
                        new Row(timestampTz(0L)),
                        new Row(timestampTz(2L)),
                        new Row(timestampTz(4L))
                )
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, /*window_end,*/ name FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.003' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.006' SECOND" +
                        "  , INTERVAL '0.003' SECOND" +
                        ")) " +
                        "GROUP BY /*window_end,*/ window_start, name",
                asList(
                        new Row(timestampTz(-3L), "Alice"),
                        new Row(timestampTz(-3L), null),
                        new Row(timestampTz(0L), "Alice"),
                        new Row(timestampTz(0L), null),
                        new Row(timestampTz(0L), "Bob"),
                        new Row(timestampTz(3L), "Alice"),
                        new Row(timestampTz(3L), "Bob")
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start, name",
                asList(
                        new Row(timestampTz(-2L)),
                        new Row(timestampTz(0L)),
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
                row(timestampTz(7), "Alice", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name || '-s' AS n FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start, name",
                asList(
                        new Row(timestampTz(-2L), "Alice-s"),
                        new Row(timestampTz(0L), "Alice-s"),
                        new Row(timestampTz(0L), "Bob-s"),
                        new Row(timestampTz(2L), "Bob-s"),
                        new Row(timestampTz(4L), "Alice-s")
                )
        );
    }

    @Test
    public void test_groupByAliasedExpression() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder("SELECT window_start, name || '-s' AS nani FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start, nani",
                asList(
                        new Row(timestampTz(-2L), "Alice-s"),
                        new Row(timestampTz(0L), "Alice-s"),
                        new Row(timestampTz(0L), "Bob-s"),
                        new Row(timestampTz(2L), "Bob-s")
                ));
    }

    @Test
    public void test_groupByWindowBoundWithExpression() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(20), null, null)
        );

        assertThatThrownBy(() -> sqlService.execute(
                "SELECT window_start + INTERVAL '0.001' SECOND, SUM(distance) FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start + INTERVAL '0.001' SECOND"))
                .hasRootCauseMessage("In window aggregation, the window_start and window_end fields must be used directly, " +
                        "without any transformation");
    }

    @Test
    public void test_groupByHaving() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Bob", 1),
                row(timestampTz(7), "Alice", 1),
                row(timestampTz(10), "Alice", 1) // flushing event
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name || '-s' AS n FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start, name " +
                        "HAVING LENGTH(n) > 5",
                asList(
                        new Row(timestampTz(-2L), "Alice-s"),
                        new Row(timestampTz(0L), "Alice-s"),
                        new Row(timestampTz(4L), "Alice-s")
                )
        );
    }

    @Test
    public void test_groupByEmpty() {
        String name = createTable();

        assertEmptyResultStream("SELECT window_start FROM " +
                "TABLE(HOP(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                "  , DESCRIPTOR(ts)" +
                "  , INTERVAL '0.004' SECOND" +
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(-2L), 1L),
                        new Row(timestampTz(0L), 3L),
                        new Row(timestampTz(2L), 2L)
                )
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, COUNT(*) FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(-2L), 2L),
                        new Row(timestampTz(0L), 4L),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(-2L), 1L),
                        new Row(timestampTz(0L), 2L),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(-2L), "Alice", 3L),
                        new Row(timestampTz(0L), "Alice", 4L),
                        new Row(timestampTz(0L), "Bob", 1L),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(-2L), "Alice", 2L),
                        new Row(timestampTz(0L), "Alice", 2L),
                        new Row(timestampTz(0L), "Bob", 1L),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start HAVING c <> 2",
                asList(
                        new Row(timestampTz(-2L), "Alice", 1L),
                        new Row(timestampTz(0L), "Alice", 1L),
                        new Row(timestampTz(2L), "Joey", 3L),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "WHERE ts > '" + timestampTz(0) + "' " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(-2L), "Bob", 1L),
                        new Row(timestampTz(0L), "Bob", 1L),
                        new Row(timestampTz(0L), "Alice", 1L),
                        new Row(timestampTz(2L), "Alice", 1L),
                        new Row(timestampTz(2L), "Joey", 2L),
                        new Row(timestampTz(4L), "Joey", 2L)
                )
        );
    }

    @Test
    public void test_countEmpty() {
        String name = createTable();

        assertEmptyResultStream(
                "SELECT window_start, COUNT(*) FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(-2L), "Alice", 1),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(-2L), "Alice"),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(-2L), "Alice", 1),
                        new Row(timestampTz(-2L), "Bob", 2),
                        new Row(timestampTz(0L), "Alice", 1),
                        new Row(timestampTz(0L), "Bob", 1),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start HAVING m > 1",
                asList(
                        new Row(timestampTz(0L), "Bob", 2),
                        new Row(timestampTz(2L), "Bob", 2),
                        new Row(timestampTz(2L), "Alice", 3),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "WHERE ts > '" + timestampTz(0) + "' " +
                        "GROUP BY window_start, name",
                asList(
                        new Row(timestampTz(-2L), "Bob", 1),
                        new Row(timestampTz(0L), "Bob", 1),
                        new Row(timestampTz(0L), "Alice", 3),
                        new Row(timestampTz(2L), "Alice", 3)
                )
        );
    }

    @Test
    public void test_minEmpty() {
        String name = createTable();

        assertEmptyResultStream(
                "SELECT window_start, MIN(name) FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(-2L), "Bob", 2),
                        new Row(timestampTz(0L), "Joey", 3),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(-2L), "Bob"),
                        new Row(timestampTz(0L), "Joey"),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(-2L), "Alice", 2),
                        new Row(timestampTz(-2L), "Bob", 1),
                        new Row(timestampTz(0L), "Alice", 2),
                        new Row(timestampTz(0L), "Bob", 2),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start HAVING m < 3",
                asList(
                        new Row(timestampTz(0L), "Bob", 2),
                        new Row(timestampTz(2L), "Bob", 2),
                        new Row(timestampTz(2L), "Alice", 1),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "WHERE ts > '" + timestampTz(0) + "' " +
                        "GROUP BY window_start, name",
                asList(
                        new Row(timestampTz(-2L), "Bob", 2),
                        new Row(timestampTz(0L), "Alice", 3),
                        new Row(timestampTz(0L), "Bob", 2),
                        new Row(timestampTz(2L), "Alice", 3)
                )
        );
    }

    @Test
    public void test_maxEmpty() {
        String name = createTable();

        assertEmptyResultStream(
                "SELECT window_start, MAX(name) FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(-2L), 1L),
                        new Row(timestampTz(0L), 3L),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(-2L), 1L),
                        new Row(timestampTz(0L), 3L),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(-2L), "Alice", 4L),
                        new Row(timestampTz(0L), "Alice", 5L),
                        new Row(timestampTz(0L), "Bob", 3L),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(-2L), "Alice", 3L),
                        new Row(timestampTz(0L), "Alice", 3L),
                        new Row(timestampTz(0L), "Bob", 3L),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start HAVING s > 1",
                asList(
                        new Row(timestampTz(0L), "Bob", 2L),
                        new Row(timestampTz(2L), "Bob", 2L),
                        new Row(timestampTz(2L), "Joey", 3L),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "WHERE ts > '" + timestampTz(0) + "' " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(-2L), "Bob", 1L),
                        new Row(timestampTz(0L), "Bob", 1L),
                        new Row(timestampTz(0L), "Alice", 1L),
                        new Row(timestampTz(2L), "Alice", 1L),
                        new Row(timestampTz(2L), "Joey", 2L),
                        new Row(timestampTz(4L), "Joey", 2L)
                )
        );
    }

    @Test
    public void test_sumEmpty() {
        String name = createTable();

        assertEmptyResultStream(
                "SELECT window_start, SUM(distance) FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start"
        );
    }

    @Test
    public void test_avg() {
        String name = createTable(
                row(timestampTz(0), "Alice", 3),
                row(timestampTz(1), null, null),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 5),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, AVG(distance) FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(-2L), new BigDecimal(3)),
                        new Row(timestampTz(0L), new BigDecimal(3)),
                        new Row(timestampTz(2L), new BigDecimal(3))
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(-2L), new BigDecimal(1)),
                        new Row(timestampTz(0L), new BigDecimal("1.5")),
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
                row(timestampTz(2), "Alice", 2),
                row(timestampTz(3), "Bob", 3),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, AVG(distance) FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(-2L), "Alice", new BigDecimal(2)),
                        new Row(timestampTz(0L), "Alice", new BigDecimal(2)),
                        new Row(timestampTz(0L), "Bob", new BigDecimal(3)),
                        new Row(timestampTz(2L), "Alice", new BigDecimal(2)),
                        new Row(timestampTz(2L), "Bob", new BigDecimal(3))
                )
        );
    }

    @Test
    public void test_avgDistinctGroupedBy() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Alice", 3),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(3), "Bob", 2),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, AVG(DISTINCT distance) FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(-2L), "Alice", new BigDecimal(2)),
                        new Row(timestampTz(0L), "Alice", new BigDecimal(2)),
                        new Row(timestampTz(0L), "Bob", new BigDecimal("1.5")),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY name, window_start HAVING a > 1",
                asList(
                        new Row(timestampTz(0L), "Bob", new BigDecimal(2)),
                        new Row(timestampTz(2L), "Bob", new BigDecimal(2)),
                        new Row(timestampTz(2L), "Joey", new BigDecimal(2)),
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
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "WHERE ts > '" + timestampTz(0) + "' " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestampTz(-2L), "Bob", new BigDecimal(1)),
                        new Row(timestampTz(0L), "Bob", new BigDecimal(1)),
                        new Row(timestampTz(0L), "Alice", new BigDecimal(1)),
                        new Row(timestampTz(2L), "Alice", new BigDecimal(1)),
                        new Row(timestampTz(2L), "Joey", new BigDecimal(2)),
                        new Row(timestampTz(4L), "Joey", new BigDecimal(2))
                )
        );
    }

    @Test
    public void test_avgEmpty() {
        String name = createTable();

        assertEmptyResultStream(
                "SELECT window_start, AVG(distance) FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start"
        );
    }

    @Test
    public void test_noOrdering() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), null, null),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(10), null, null)
        );

        assertThatThrownBy(() -> sqlService.execute(
                "SELECT window_start, SUM(distance) FROM " +
                        "TABLE(HOP(TABLE " + name +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) GROUP BY window_start"))
                .hasRootCauseMessage("Streaming aggregation is supported only for window aggregation, with imposed watermark order " +
                        "(see TUMBLE/HOP and IMPOSE_ORDER functions)");
    }

    @Test
    public void test_withFullProjection() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.001' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.002' SECOND" +
                        "  , INTERVAL '0.001' SECOND" +
                        ")) " +
                        "GROUP BY window_start, window_end, ts, name, distance",
                asList(
                        new Row(timestampTz(0L), "Alice", 1, timestampTz(-1L), timestampTz(1L)),
                        new Row(timestampTz(0L), "Alice", 1, timestampTz(0L), timestampTz(2L))
                )
        );
    }

    @Test
    public void test_multipleAggregations() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Bob", 1),
                row(timestampTz(3), "Alice", 1),
                row(timestampTz(5), "Bob", 3),
                row(timestampTz(5), "Joey", 5),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, COUNT(*), MIN(name), MAX(name), SUM(distance), AVG(distance) FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(-2L), 2L, "Alice", "Bob", 2L, new BigDecimal(1)),
                        new Row(timestampTz(0L), 3L, "Alice", "Bob", 3L, new BigDecimal(1)),
                        new Row(timestampTz(2L), 3L, "Alice", "Joey", 9L, new BigDecimal(3)),
                        new Row(timestampTz(4L), 2L, "Bob", "Joey", 8L, new BigDecimal(4))
                )
        );
    }

    @Test
    public void test_ordering_tinyInt() {
        checkOrdering(TINYINT, "2", row((byte) 0), row((byte) 4));
    }

    @Test
    public void test_ordering_smallInt() {
        checkOrdering(SMALLINT, "2", row((short) 0), row((short) 4));
    }

    @Test
    public void test_ordering_int() {
        checkOrdering(INTEGER, "2", row(0), row(4));
    }

    @Test
    public void test_ordering_bigInt() {
        checkOrdering(BIGINT, "2", row(0L), row(4L));
    }

    @Test
    public void test_ordering_time() {
        checkOrdering(TIME, "INTERVAL '0.002' SECOND", row(time(0)), row(time(4)));
    }

    @Test
    public void test_ordering_date() {
        checkOrdering(DATE, "INTERVAL '2' DAYS", row(date(0)), row(date(345_600_000)));
    }

    @Test
    public void test_ordering_timestamp() {
        checkOrdering(TIMESTAMP, "INTERVAL '0.002' SECOND", row(timestamp(0)), row(timestamp(4)));
    }

    @Test
    public void test_ordering_timestampTz() {
        checkOrdering(TIMESTAMP_WITH_TIME_ZONE, "INTERVAL '0.002' SECOND", row(timestampTz(0)), row(timestampTz(4)));
    }

    private static void checkOrdering(QueryDataTypeFamily orderingColumnType, String size, Object[]... values) {
        String name = randomName();
        TestStreamSqlConnector.create(sqlService, name, singletonList("ts"), singletonList(orderingColumnType), values);

        assertRowsEventuallyInAnyOrder(
                "SELECT COUNT(*) FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), " + size + ")))" +
                        "  , DESCRIPTOR(ts)" +
                        "  , " + size +
                        "  , " + size +
                        ")) " +
                        "GROUP BY window_start",
                singletonList(new Row(1L))
        );
    }

    @Test
    public void test_nested_filter() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(2), "Alice", 3),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start_inner, name, COUNT(name) FROM " +
                        "TABLE(HOP(" +
                        "   (SELECT ts, name, window_start AS window_start_inner FROM" +
                        "      TABLE(HOP(" +
                        "           (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        "           , DESCRIPTOR(ts)" +
                        "           , INTERVAL '0.004' SECOND" +
                        "           , INTERVAL '0.002' SECOND" +
                        "       )) WHERE ts > '" + timestampTz(0) + "' " +
                        "   )" +
                        "   , DESCRIPTOR(ts)" +
                        "   , INTERVAL '0.004' SECOND" +
                        "   , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start_inner, name",
                asList(
                        new Row(timestampTz(0L), "Alice", 1L),
                        new Row(timestampTz(0L), "Alice", 1L),
                        new Row(timestampTz(0L), "Bob", 1L),
                        new Row(timestampTz(0L), "Bob", 1L),
                        new Row(timestampTz(2L), "Alice", 1L),
                        new Row(timestampTz(2L), "Alice", 1L),
                        new Row(timestampTz(2L), "Bob", 1L),
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
                "SELECT window_start, window_end, wsi, wei, name, COUNT(name) FROM " +
                        "TABLE(HOP(" +
                        "   (SELECT ts, name, window_start AS wsi, window_end AS wei FROM " +
                        "      TABLE(HOP(" +
                        "           (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.001' SECOND)))" +
                        "           , DESCRIPTOR(ts)" +
                        "           , INTERVAL '0.002' SECOND" +
                        "           , INTERVAL '0.001' SECOND" +
                        "       ))" +
                        "   )" +
                        "   , DESCRIPTOR(ts)" +
                        "   , INTERVAL '0.004' SECOND" +
                        "   , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start, window_end, wsi, wei, name",
                asList(
                        new Row(timestampTz(-2L), timestampTz(2L), timestampTz(-1L), timestampTz(1L), "Alice", 1L),
                        new Row(timestampTz(-2L), timestampTz(2L), timestampTz(0L), timestampTz(2L), "Alice", 2L),
                        new Row(timestampTz(-2L), timestampTz(2L), timestampTz(1L), timestampTz(3L), "Alice", 1L),
                        new Row(timestampTz(0L), timestampTz(4L), timestampTz(-1L), timestampTz(1L), "Alice", 1L),
                        new Row(timestampTz(0L), timestampTz(4L), timestampTz(0L), timestampTz(2L), "Alice", 2L),
                        new Row(timestampTz(0L), timestampTz(4L), timestampTz(1L), timestampTz(3L), "Alice", 2L),
                        new Row(timestampTz(0L), timestampTz(4L), timestampTz(2L), timestampTz(4L), "Alice", 1L),
                        new Row(timestampTz(0L), timestampTz(4L), timestampTz(2L), timestampTz(4L), "Bob", 1L),
                        new Row(timestampTz(0L), timestampTz(4L), timestampTz(3L), timestampTz(5L), "Bob", 1L),
                        new Row(timestampTz(2L), timestampTz(6L), timestampTz(1L), timestampTz(3L), "Alice", 1L),
                        new Row(timestampTz(2L), timestampTz(6L), timestampTz(2L), timestampTz(4L), "Alice", 1L),
                        new Row(timestampTz(2L), timestampTz(6L), timestampTz(2L), timestampTz(4L), "Bob", 1L),
                        new Row(timestampTz(2L), timestampTz(6L), timestampTz(3L), timestampTz(5L), "Bob", 1L)
                )
        );
    }

    @Test
    public void test_nested_aggregate() {
        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Bob", 1),
                row(timestampTz(1), "Alice", 1),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_end, window_end_inner, name, COUNT(name) FROM " +
                        "TABLE(HOP(" +
                        "   (SELECT name, window_end AS window_end_inner FROM " +
                        "       TABLE(HOP(" +
                        "           (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.001' SECOND)))" +
                        "           , DESCRIPTOR(ts)" +
                        "           , INTERVAL '0.002' SECOND" +
                        "           , INTERVAL '0.001' SECOND" +
                        "       )) GROUP BY name, window_end_inner" +
                        "   )" +
                        "   , DESCRIPTOR(window_end_inner)" +
                        "   , INTERVAL '0.004' SECOND" +
                        "   , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_end, window_end_inner, name",
                asList(
                        new Row(timestampTz(2L), timestampTz(1L), "Alice", 1L),
                        new Row(timestampTz(4L), timestampTz(1L), "Alice", 1L),
                        new Row(timestampTz(4L), timestampTz(2L), "Alice", 1L),
                        new Row(timestampTz(4L), timestampTz(2L), "Bob", 1L),
                        new Row(timestampTz(4L), timestampTz(3L), "Alice", 1L),
                        new Row(timestampTz(4L), timestampTz(3L), "Bob", 1L),
                        new Row(timestampTz(6L), timestampTz(2L), "Alice", 1L),
                        new Row(timestampTz(6L), timestampTz(2L), "Bob", 1L),
                        new Row(timestampTz(6L), timestampTz(3L), "Alice", 1L),
                        new Row(timestampTz(6L), timestampTz(3L), "Bob", 1L),
                        new Row(timestampTz(6L), timestampTz(4L), "Alice", 1L),
                        new Row(timestampTz(6L), timestampTz(4L), "Bob", 1L),
                        new Row(timestampTz(6L), timestampTz(5L), "Bob", 1L),
                        new Row(timestampTz(8L), timestampTz(4L), "Alice", 1L),
                        new Row(timestampTz(8L), timestampTz(4L), "Bob", 1L),
                        new Row(timestampTz(8L), timestampTz(5L), "Bob", 1L)
                )
        );
    }

    @Test
    public void test_nested_join() {
        createMapping("map", OffsetDateTime.class, String.class);
        instance().getMap("map").put(timestampTz(0), "value-0");
        instance().getMap("map").put(timestampTz(1), "value-1");
        instance().getMap("map").put(timestampTz(2), "value-2");

        String name = createTable(
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), "Bob", 1),
                row(timestampTz(2), "Joey", 1),
                row(timestampTz(10), null, null)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_start_inner, this, COUNT(*) FROM " +
                        "TABLE(HOP(" +
                        "   (SELECT ts, window_start window_start_inner, name, this FROM " +
                        "       TABLE(HOP(" +
                        "           (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.001' SECOND)))" +
                        "           , DESCRIPTOR(ts)" +
                        "           , INTERVAL '0.002' SECOND" +
                        "           , INTERVAL '0.001' SECOND" +
                        "       )) JOIN map ON ts = __key" +
                        "   )" +
                        "   , DESCRIPTOR(ts)" +
                        "   , INTERVAL '0.004' SECOND" +
                        "   , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start, window_start_inner, this",
                asList(
                        new Row(timestampTz(-2L), timestampTz(-1L), "value-0", 1L),
                        new Row(timestampTz(-2L), timestampTz(0L), "value-0", 1L),
                        new Row(timestampTz(-2L), timestampTz(0L), "value-1", 1L),
                        new Row(timestampTz(-2L), timestampTz(1L), "value-1", 1L),
                        new Row(timestampTz(0L), timestampTz(-1L), "value-0", 1L),
                        new Row(timestampTz(0L), timestampTz(0L), "value-0", 1L),
                        new Row(timestampTz(0L), timestampTz(0L), "value-1", 1L),
                        new Row(timestampTz(0L), timestampTz(1L), "value-1", 1L),
                        new Row(timestampTz(0L), timestampTz(1L), "value-2", 1L),
                        new Row(timestampTz(0L), timestampTz(2L), "value-2", 1L),
                        new Row(timestampTz(2L), timestampTz(1L), "value-2", 1L),
                        new Row(timestampTz(2L), timestampTz(2L), "value-2", 1L)
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
                "SELECT window_start, /*window_end,*/ COUNT(name) FROM " +
                        "TABLE(HOP(" +
                        "   time_col => DESCRIPTOR(ts)" +
                        "   , window_size => INTERVAL '0.004' SECOND" +
                        "   , slide_size =>  INTERVAL '0.002' SECOND" +
                        "   , input => (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                        ")) " +
                        "GROUP BY window_start/*, window_end*/",
                asList(
                        new Row(timestampTz(-2L), 1L),
                        new Row(timestampTz(0L), 3L),
                        new Row(timestampTz(2L), 2L)
                )
        );
    }

    @Test
    public void test_groupByWithoutOrdering() {
        String name = createTable();

        assertThatThrownBy(() -> sqlService.execute("SELECT window_start FROM " +
                "TABLE(HOP(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.001' SECOND, INTERVAL '0.002' SECOND)) " +
                "GROUP BY window_start")
        ).hasRootCauseMessage("Streaming aggregation is supported only for window aggregation, with imposed watermark order " +
                "(see TUMBLE/HOP and IMPOSE_ORDER functions)");
    }

    @Test
    public void test_aggregationWithoutGrouping() {
        String name = createTable();

        assertThatThrownBy(() -> sqlService.execute("SELECT COUNT(*) FROM " +
                "TABLE(HOP(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.001' SECOND, INTERVAL '0.002' SECOND)) " +
                "GROUP BY window_start")
        ).hasRootCauseMessage("Streaming aggregation is supported only for window aggregation, with imposed watermark order " +
                "(see TUMBLE/HOP and IMPOSE_ORDER functions)");
    }

    @Test
    public void test_aggregationWithoutOrderingAndGrouping() {
        String name = createTable();

        assertThatThrownBy(() -> sqlService.execute("SELECT COUNT(*) FROM " +
                "TABLE(HOP(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.001' SECOND, INTERVAL '0.002' SECOND))")
        ).hasRootCauseMessage("Streaming aggregation is supported only for window aggregation, with imposed watermark order " +
                "(see TUMBLE/HOP and IMPOSE_ORDER functions)");
    }

    @Test
    public void test_noGroupBy() {
        String name = createTable();

        assertThatThrownBy(() -> sqlService.execute("SELECT COUNT(*) FROM " +
                "TABLE(HOP(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                "  , DESCRIPTOR(ts)" +
                "  , INTERVAL '0.004' SECOND" +
                "  , INTERVAL '0.002' SECOND" +
                "))")
        ).hasRootCauseMessage("Streaming aggregation is supported only for window aggregation, with imposed watermark order " +
                "(see TUMBLE/HOP and IMPOSE_ORDER functions)");
    }

    @Test
    public void test_groupByNonWindowBound() {
        String name = createTable();

        assertThatThrownBy(() -> sqlService.execute("SELECT window_start, SUM(distance) FROM " +
                "TABLE(HOP(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                "  , DESCRIPTOR(ts)" +
                "  , INTERVAL '0.004' SECOND" +
                "  , INTERVAL '0.002' SECOND" +
                ")) " +
                "GROUP BY window_start + INTERVAL '0.001' SECOND")
        ).hasMessageContaining("Expression 'window_start' is not being grouped");
    }

    @Test
    public void test_batchSource() {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name"),
                asList(TIMESTAMP_WITH_TIME_ZONE, VARCHAR),
                TestBatchSqlConnector.valuesToString(
                        row(timestampTz(0), "Alice"),
                        row(timestampTz(1), null),
                        row(timestampTz(2), "Alice"),
                        row(timestampTz(3), "Bob")));

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end, COUNT(name) FROM " +
                        "TABLE(HOP(" +
                        "  TABLE " + name +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start, window_end",
                asList(
                        new Row(timestampTz(-2L), timestampTz(2L), 1L),
                        new Row(timestampTz(0L), timestampTz(4L), 3L),
                        new Row(timestampTz(2L), timestampTz(6L), 2L)
                )
        );
    }

    @Test
    public void test_batchSource_noGroupBy() {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("ts", "id"),
                asList(TIMESTAMP_WITH_TIME_ZONE, INTEGER),
                TestBatchSqlConnector.valuesToString(
                        row(timestampTz(2), 1),
                        row(timestampTz(4), 2),
                        row(timestampTz(6), 3)));

        assertRowsEventuallyInAnyOrder(
                "SELECT SUM(id) FROM " +
                        "TABLE(HOP(" +
                        "  TABLE " + name +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.2' SECOND" +
                        "  , INTERVAL '0.1' SECOND" +
                        "))",
                singletonList(new Row(12L))
        );
    }

    @Test
    public void test_batchSource_groupByNonWindowBound() {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name"),
                asList(TIMESTAMP_WITH_TIME_ZONE, VARCHAR),
                TestBatchSqlConnector.valuesToString(
                        row(timestampTz(0), "Alice"),
                        row(timestampTz(1), null),
                        row(timestampTz(2), "Alice"),
                        row(timestampTz(3), "Bob")));

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start + INTERVAL '0.001' SECOND FROM " +
                        "TABLE(HOP(" +
                        "  TABLE " + name +
                        "  , DESCRIPTOR(ts)" +
                        "  , INTERVAL '0.004' SECOND" +
                        "  , INTERVAL '0.002' SECOND" +
                        ")) " +
                        "GROUP BY window_start + INTERVAL '0.001' SECOND",
                asList(
                        new Row(timestampTz(-1L)),
                        new Row(timestampTz(1L)),
                        new Row(timestampTz(3L))
                )
        );
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
