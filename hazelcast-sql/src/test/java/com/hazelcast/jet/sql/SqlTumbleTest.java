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
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.OffsetDateTime;

import static com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector.timestamp;
import static com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector.watermark;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class SqlTumbleTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() throws IOException {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_windowMetadata() {
        String name = createTable();

        try (SqlResult result = sqlService.execute("SELECT * FROM " +
                "TABLE(TUMBLE(TABLE " + name + " , DESCRIPTOR(ts), INTERVAL '1' DAY))")
        ) {
            assertThat(result.getRowMetadata().findColumn("window_start")).isGreaterThan(-1);
            assertThat(result.getRowMetadata().getColumn(3).getType()).isEqualTo(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE);
            assertThat(result.getRowMetadata().findColumn("window_end")).isGreaterThan(-1);
            assertThat(result.getRowMetadata().getColumn(4).getType()).isEqualTo(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE);
        }
    }

    @Test
    public void test_groupBy() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), null, 1),
                row(timestamp(2), "Alice", 1),
                row(timestamp(3), "Bob", 1),
                row(timestamp(4), "Alice", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY 1, 2", // field ordinals
                asList(
                        new Row(timestamp(0L), timestamp(2L)),
                        new Row(timestamp(2L), timestamp(4L)),
                        new Row(timestamp(4L), timestamp(6L))
                )
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end, name FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.003' SECOND)) " +
                        "GROUP BY window_end, window_start, name",
                asList(
                        new Row(timestamp(0L), timestamp(3L), "Alice"),
                        new Row(timestamp(0L), timestamp(3L), null),
                        new Row(timestamp(3L), timestamp(6L), "Bob"),
                        new Row(timestamp(3L), timestamp(6L), "Alice")
                )
        );
    }

    @Test
    public void test_groupByNotSelectedField() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Bob", 1),
                row(timestamp(3), "Alice", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start, name",
                asList(
                        new Row(timestamp(0L)),
                        new Row(timestamp(2L)),
                        new Row(timestamp(2L))
                )
        );
    }

    @Test
    public void test_groupByExpression() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Bob", 1),
                row(timestamp(3), "Alice", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name || '-s' AS n FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start, n",
                asList(
                        new Row(timestamp(0L), "Alice-s"),
                        new Row(timestamp(2L), "Bob-s"),
                        new Row(timestamp(2L), "Alice-s")
                )
        );
    }

    @Test
    public void test_groupByHaving() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Bob", 1),
                row(timestamp(3), "Alice", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name || '-s' AS n FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start, name " +
                        "HAVING LENGTH(n) > 5",
                asList(
                        new Row(timestamp(0L), "Alice-s"),
                        new Row(timestamp(2L), "Alice-s")
                )
        );
    }

    @Test
    public void test_groupByEmpty() {
        String name = createTable(watermark(10));

        assertEmptyResult("SELECT window_start FROM " +
                "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                "GROUP BY window_start"
        );
    }

    @Test
    public void test_count() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), null, null),
                row(timestamp(2), "Alice", 1),
                row(timestamp(3), "Bob", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, COUNT(name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestamp(0L), 1L),
                        new Row(timestamp(2L), 2L)
                )
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, COUNT(*) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestamp(0L), 2L),
                        new Row(timestamp(2L), 2L)
                )
        );
    }

    @Test
    public void test_countDistinct() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Bob", 1),
                row(timestamp(3), "Alice", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, COUNT(DISTINCT name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestamp(0L), 1L),
                        new Row(timestamp(2L), 2L)
                )
        );
    }

    @Test
    public void test_countGroupedBy() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Alice", 2),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Alice", 1),
                row(timestamp(3), "Bob", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, COUNT(distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestamp(0L), "Alice", 3L),
                        new Row(timestamp(2L), "Alice", 1L),
                        new Row(timestamp(2L), "Bob", 1L)
                )
        );
    }

    @Test
    public void test_countDistinctGroupedBy() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Alice", 2),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Alice", 1),
                row(timestamp(3), "Bob", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, COUNT(DISTINCT distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestamp(0L), "Alice", 2L),
                        new Row(timestamp(2L), "Alice", 1L),
                        new Row(timestamp(2L), "Bob", 1L)
                )
        );
    }

    @Test
    public void test_countGroupedByHaving() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(2), "Bob", 1),
                row(timestamp(3), "Bob", 1),
                row(timestamp(4), "Joey", 1),
                row(timestamp(5), "Joey", 1),
                row(timestamp(5), "Joey", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, COUNT(*) c FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start HAVING c <> 2",
                asList(
                        new Row(timestamp(0L), "Alice", 1L),
                        new Row(timestamp(4L), "Joey", 3L)
                )
        );
    }

    @Test
    public void test_countFilter() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Bob", 1),
                row(timestamp(3), "Alice", 1),
                row(timestamp(5), "Joey", 1),
                row(timestamp(5), "Joey", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, COUNT(name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "WHERE ts > '1970-01-01T00:00:00.000Z' " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestamp(0L), "Bob", 1L),
                        new Row(timestamp(2L), "Alice", 1L),
                        new Row(timestamp(4L), "Joey", 2L)
                )
        );
    }

    @Test
    public void test_countEmpty() {
        String name = createTable(watermark(10));

        assertEmptyResult(
                "SELECT window_start, COUNT(*) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start"
        );
    }

    @Test
    public void test_min() {
        String name = createTable(
                row(timestamp(0), "Alice", 2),
                row(timestamp(0), "Bob", 1),
                row(timestamp(1), null, null),
                row(timestamp(2), "Bob", 2),
                row(timestamp(3), "Joey", 3),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, MIN(name), MIN(distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestamp(0L), "Alice", 1),
                        new Row(timestamp(2L), "Bob", 2)
                )
        );
    }

    @Test
    public void test_minDistinct() {
        String name = createTable(
                row(timestamp(0), "Bob", 1),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Bob", 1),
                row(timestamp(3), "Joey", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, MIN(DISTINCT name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestamp(0L), "Alice"),
                        new Row(timestamp(2L), "Bob")
                )
        );
    }

    @Test
    public void test_minGroupedBy() {
        String name = createTable(
                row(timestamp(0), "Alice", 2),
                row(timestamp(1), "Bob", 2),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Alice", 2),
                row(timestamp(2), "Bob", 1),
                row(timestamp(3), "Bob", 2),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, MIN(distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestamp(0L), "Alice", 1),
                        new Row(timestamp(0L), "Bob", 2),
                        new Row(timestamp(2L), "Alice", 2),
                        new Row(timestamp(2L), "Bob", 1)
                )
        );
    }

    @Test
    public void test_minGroupedByHaving() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(2), "Bob", 2),
                row(timestamp(3), "Bob", 3),
                row(timestamp(4), "Alice", 3),
                row(timestamp(4), "Alice", 4),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, MIN(distance) m FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start HAVING m > 1",
                asList(
                        new Row(timestamp(2L), "Bob", 2),
                        new Row(timestamp(4L), "Alice", 3)
                )
        );
    }

    @Test
    public void test_minFilter() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Bob", 2),
                row(timestamp(1), "Bob", 1),
                row(timestamp(3), "Alice", 3),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, MIN(distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "WHERE ts > '1970-01-01T00:00:00.000Z' " +
                        "GROUP BY window_start, name",
                asList(
                        new Row(timestamp(0L), "Bob", 1),
                        new Row(timestamp(2L), "Alice", 3)
                )
        );
    }

    @Test
    public void test_minEmpty() {
        String name = createTable(watermark(10));

        assertEmptyResult(
                "SELECT window_start, MIN(name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start"
        );
    }

    @Test
    public void test_max() {
        String name = createTable(
                row(timestamp(0), "Alice", 2),
                row(timestamp(0), "Bob", 1),
                row(timestamp(1), null, null),
                row(timestamp(2), "Bob", 2),
                row(timestamp(3), "Joey", 3),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, MAX(name), MAX(distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestamp(0L), "Bob", 2),
                        new Row(timestamp(2L), "Joey", 3)
                )
        );
    }

    @Test
    public void test_maxDistinct() {
        String name = createTable(
                row(timestamp(0), "Bob", 1),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Bob", 1),
                row(timestamp(3), "Joey", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, MAX(DISTINCT name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestamp(0L), "Bob"),
                        new Row(timestamp(2L), "Joey")
                )
        );
    }

    @Test
    public void test_maxGroupedBy() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Bob", 1),
                row(timestamp(1), "Alice", 2),
                row(timestamp(2), "Alice", 1),
                row(timestamp(2), "Bob", 2),
                row(timestamp(3), "Bob", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, MAX(distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestamp(0L), "Alice", 2),
                        new Row(timestamp(0L), "Bob", 1),
                        new Row(timestamp(2L), "Alice", 1),
                        new Row(timestamp(2L), "Bob", 2)
                )
        );
    }

    @Test
    public void test_maxGroupedByHaving() {
        String name = createTable(
                row(timestamp(0), "Alice", 3),
                row(timestamp(2), "Bob", 2),
                row(timestamp(3), "Bob", 1),
                row(timestamp(4), "Alice", 1),
                row(timestamp(4), "Alice", 0),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, MAX(distance) m FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start HAVING m < 3",
                asList(
                        new Row(timestamp(2L), "Bob", 2),
                        new Row(timestamp(4L), "Alice", 1)
                )
        );
    }

    @Test
    public void test_maxFilter() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Bob", 2),
                row(timestamp(1), "Bob", 1),
                row(timestamp(3), "Alice", 3),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, MAX(distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "WHERE ts > '1970-01-01T00:00:00.000Z' " +
                        "GROUP BY window_start, name",
                asList(
                        new Row(timestamp(0L), "Bob", 2),
                        new Row(timestamp(2L), "Alice", 3)
                )
        );
    }

    @Test
    public void test_maxEmpty() {
        String name = createTable(watermark(10));

        assertEmptyResult(
                "SELECT window_start, MAX(name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start"
        );
    }

    @Test
    public void test_sum() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), null, null),
                row(timestamp(2), "Alice", 1),
                row(timestamp(3), "Bob", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, SUM(distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestamp(0L), 1L),
                        new Row(timestamp(2L), 2L)
                )
        );
    }

    @Test
    public void test_sumDistinct() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Bob", 1),
                row(timestamp(3), "Alice", 2),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, SUM(DISTINCT distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestamp(0L), 1L),
                        new Row(timestamp(2L), 3L)
                )
        );
    }

    @Test
    public void test_sumGroupedBy() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Alice", 2),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Alice", 1),
                row(timestamp(3), "Bob", 1),
                row(timestamp(3), "Bob", 2),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, SUM(distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestamp(0L), "Alice", 4L),
                        new Row(timestamp(2L), "Alice", 1L),
                        new Row(timestamp(2L), "Bob", 3L)
                )
        );
    }

    @Test
    public void test_sumDistinctGroupedBy() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Alice", 2),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Alice", 1),
                row(timestamp(3), "Bob", 1),
                row(timestamp(3), "Bob", 2),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, SUM(DISTINCT distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestamp(0L), "Alice", 3L),
                        new Row(timestamp(2L), "Alice", 1L),
                        new Row(timestamp(2L), "Bob", 3L)
                )
        );
    }

    @Test
    public void test_sumGroupedByHaving() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(2), "Bob", 1),
                row(timestamp(3), "Bob", 1),
                row(timestamp(4), "Joey", 1),
                row(timestamp(5), "Joey", 1),
                row(timestamp(5), "Joey", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, SUM(distance) s FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start HAVING s > 1",
                asList(
                        new Row(timestamp(2L), "Bob", 2L),
                        new Row(timestamp(4L), "Joey", 3L)
                )
        );
    }

    @Test
    public void test_sumFilter() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Bob", 1),
                row(timestamp(3), "Alice", 1),
                row(timestamp(5), "Joey", 1),
                row(timestamp(5), "Joey", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, SUM(distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "WHERE ts > '1970-01-01T00:00:00.000Z' " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestamp(0L), "Bob", 1L),
                        new Row(timestamp(2L), "Alice", 1L),
                        new Row(timestamp(4L), "Joey", 2L)
                )
        );
    }

    @Test
    public void test_sumEmpty() {
        String name = createTable(watermark(10));

        assertEmptyResult(
                "SELECT window_start, SUM(distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start"
        );
    }

    @Test
    public void test_avg() {
        String name = createTable(
                row(timestamp(0), "Alice", 2),
                row(timestamp(1), null, null),
                row(timestamp(2), "Alice", 1),
                row(timestamp(3), "Bob", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, AVG(distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestamp(0L), new BigDecimal(2)),
                        new Row(timestamp(2L), new BigDecimal(1))
                )
        );
    }

    @Test
    public void test_avgDistinct() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Bob", 1),
                row(timestamp(3), "Alice", 2),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, AVG(DISTINCT distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestamp(0L), new BigDecimal(1)),
                        new Row(timestamp(2L), new BigDecimal("1.5"))
                )
        );
    }

    @Test
    public void test_avgGroupedBy() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Alice", 4),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Alice", 1),
                row(timestamp(3), "Bob", 1),
                row(timestamp(3), "Bob", 2),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, AVG(distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestamp(0L), "Alice", new BigDecimal(2)),
                        new Row(timestamp(2L), "Alice", new BigDecimal(1)),
                        new Row(timestamp(2L), "Bob", new BigDecimal("1.5"))
                )
        );
    }

    @Test
    public void test_avgDistinctGroupedBy() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Alice", 2),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Alice", 1),
                row(timestamp(3), "Bob", 1),
                row(timestamp(3), "Bob", 2),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, AVG(DISTINCT distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestamp(0L), "Alice", new BigDecimal("1.5")),
                        new Row(timestamp(2L), "Alice", new BigDecimal(1)),
                        new Row(timestamp(2L), "Bob", new BigDecimal("1.5"))
                )
        );
    }

    @Test
    public void test_avgGroupedByHaving() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Bob", 1),
                row(timestamp(3), "Bob", 3),
                row(timestamp(4), "Joey", 1),
                row(timestamp(5), "Joey", 4),
                row(timestamp(5), "Joey", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, AVG(distance) a FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY name, window_start HAVING a > 1",
                asList(
                        new Row(timestamp(2L), "Bob", new BigDecimal(2)),
                        new Row(timestamp(4L), "Joey", new BigDecimal(2))
                )
        );
    }

    @Test
    public void test_avgFilter() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Bob", 1),
                row(timestamp(3), "Alice", 1),
                row(timestamp(5), "Joey", 3),
                row(timestamp(5), "Joey", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, name, AVG(distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "WHERE ts > '1970-01-01T00:00:00.000Z' " +
                        "GROUP BY name, window_start",
                asList(
                        new Row(timestamp(0L), "Bob", new BigDecimal(1)),
                        new Row(timestamp(2L), "Alice", new BigDecimal(1)),
                        new Row(timestamp(4L), "Joey", new BigDecimal(2))
                )
        );
    }

    @Test
    public void test_avgEmpty() {
        String name = createTable(watermark(10));

        assertEmptyResult(
                "SELECT window_start, AVG(distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start"
        );
    }

    @Test
    public void test_multipleAggregations() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Bob", 1),
                row(timestamp(3), "Alice", 1),
                row(timestamp(5), "Bob", 3),
                row(timestamp(5), "Joey", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, COUNT(*), MIN(name), MAX(name), SUM(distance), AVG(distance) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestamp(0L), 2L, "Alice", "Bob", 2L, new BigDecimal(1)),
                        new Row(timestamp(2L), 1L, "Alice", "Alice", 1L, new BigDecimal(1)),
                        new Row(timestamp(4L), 2L, "Bob", "Joey", 4L, new BigDecimal(2))
                )
        );
    }

    @Test
    public void test_nested_filter() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Alice", 1),
                row(timestamp(3), "Bob", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start_inner, name, COUNT(name) FROM " +
                        "TABLE(TUMBLE(" +
                        "   (SELECT ts, name, window_start window_start_inner FROM" +
                        "      TABLE(TUMBLE(" +
                        "           TABLE " + name +
                        "           , DESCRIPTOR(ts)" +
                        "           , INTERVAL '0.002' SECOND" +
                        "       )) WHERE ts > '1970-01-01T00:00:00.000Z'" +
                        "   )" +
                        "   , DESCRIPTOR(ts)" +
                        "   , INTERVAL '0.003' SECOND" +
                        ")) " +
                        "GROUP BY window_start_inner, name",
                asList(
                        new Row(timestamp(0L), "Alice", 1L),
                        new Row(timestamp(2L), "Alice", 1L),
                        new Row(timestamp(2L), "Bob", 1L)
                )
        );
    }

    @Test
    public void test_nested_project() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Alice", 1),
                row(timestamp(2), "Alice", 1),
                row(timestamp(3), "Bob", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start_inner_2, window_start_inner_1, name, COUNT(name) FROM " +
                        "TABLE(TUMBLE(" +
                        "   (SELECT ts, name, window_start window_start_inner_1, window_start window_start_inner_2 FROM" +
                        "      TABLE(TUMBLE(" +
                        "           TABLE " + name +
                        "           , DESCRIPTOR(ts)" +
                        "           , INTERVAL '0.002' SECOND" +
                        "       ))" +
                        "   )" +
                        "   , DESCRIPTOR(ts)" +
                        "   , INTERVAL '0.003' SECOND" +
                        ")) " +
                        "GROUP BY window_start_inner_2, window_start_inner_1, name",
                asList(
                        new Row(timestamp(0L), timestamp(0L), "Alice", 2L),
                        new Row(timestamp(2L), timestamp(2L), "Alice", 1L),
                        new Row(timestamp(2L), timestamp(2L), "Bob", 1L)
                )
        );
    }

    @Test
    public void test_nested_aggregate() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Bob", 1),
                row(timestamp(2), "Alice", 1),
                row(timestamp(3), "Alice", 1),
                row(timestamp(5), "Bob", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start_inner, name, COUNT(*) FROM " +
                        "TABLE(TUMBLE(" +
                        "   (SELECT name, window_start window_start_inner FROM " +
                        "       TABLE(TUMBLE(" +
                        "           TABLE " + name +
                        "           , DESCRIPTOR(ts)" +
                        "           , INTERVAL '0.002' SECOND" +
                        "       )) GROUP BY name, window_start_inner" +
                        "   )" +
                        "   , DESCRIPTOR(window_start_inner)" +
                        "   , INTERVAL '0.003' SECOND" +
                        ")) " +
                        "GROUP BY window_start_inner, name",
                asList(
                        new Row(timestamp(0L), "Alice", 1L),
                        new Row(timestamp(0L), "Bob", 1L),
                        new Row(timestamp(2L), "Alice", 1L),
                        new Row(timestamp(4L), "Bob", 1L)
                )
        );
    }

    @Test
    public void test_nested_join() {
        createMapping("map", OffsetDateTime.class, String.class);
        instance().getMap("map").put(timestamp(0), "value-0");
        instance().getMap("map").put(timestamp(1), "value-1");
        instance().getMap("map").put(timestamp(2), "value-1");
        instance().getMap("map").put(timestamp(3), "value-1");

        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(1), "Bob", 1),
                row(timestamp(2), "Joey", 1),
                row(timestamp(3), "Alice", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start_inner, this, COUNT(*) FROM " +
                        "TABLE(TUMBLE(" +
                        "   (SELECT ts, window_start window_start_inner, this FROM " +
                        "       TABLE(TUMBLE(" +
                        "           TABLE " + name +
                        "           , DESCRIPTOR(ts)" +
                        "           , INTERVAL '0.002' SECOND" +
                        "       )) JOIN map ON ts = __key" +
                        "   )" +
                        "   , DESCRIPTOR(ts)" +
                        "   , INTERVAL '0.003' SECOND" +
                        ")) " +
                        "GROUP BY window_start_inner, this",
                asList(
                        new Row(timestamp(0L), "value-0", 1L),
                        new Row(timestamp(0L), "value-1", 1L),
                        new Row(timestamp(2L), "value-1", 2L)
                )
        );
    }

    @Test
    public void test_namedParameters() {
        String name = createTable(
                row(timestamp(0), "Alice", 1),
                row(timestamp(2), "Alice", 1),
                row(timestamp(3), "Bob", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end, COUNT(name) FROM " +
                        "TABLE(TUMBLE(" +
                        "   window_size => INTERVAL '0.002' SECOND" +
                        "   , time_column => DESCRIPTOR(ts)" +
                        "   , input => (TABLE " + name + ")" +
                        ")) " +
                        "GROUP BY window_start, window_end",
                asList(
                        new Row(timestamp(0L), timestamp(2L), 1L),
                        new Row(timestamp(2L), timestamp(4L), 2L)
                )
        );
    }

    @Test
    @Ignore("flaky") // TODO:
    public void test_dropLateRows() {
        String name = createTable(
                row(timestamp(3), "Alice", 1),
                row(timestamp(2), "Bob", 1),
                row(timestamp(1), "Alice", 1),
                watermark(2),
                row(timestamp(0), "Alice", 1),
                row(timestamp(2), "Bob", 1),
                watermark(10)
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, COUNT(*) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestamp(0L), 1L),
                        new Row(timestamp(2L), 3L)
                )
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
                row(timestamp(0), "Alice"),
                row(timestamp(1), null),
                row(timestamp(2), "Alice"),
                row(timestamp(3), "Bob")
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end, COUNT(name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start, window_end",
                asList(
                        new Row(timestamp(0L), timestamp(2L), 1L),
                        new Row(timestamp(2L), timestamp(4L), 2L)
                )
        );
    }

    @Test
    public void when_windowEdgeIsProjected_then_regularAggregationIsApplied() {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name"),
                asList(TIMESTAMP_WITH_TIME_ZONE, VARCHAR),
                row(timestamp(0), "Alice"),
                row(timestamp(1), null),
                row(timestamp(2), "Alice"),
                row(timestamp(3), "Bob")
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start + INTERVAL '0.001' SECOND, COUNT(name) FROM " +
                        "TABLE(TUMBLE(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start + INTERVAL '0.001' SECOND",
                asList(
                        new Row(timestamp(1L), 1L),
                        new Row(timestamp(3L), 2L)
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
