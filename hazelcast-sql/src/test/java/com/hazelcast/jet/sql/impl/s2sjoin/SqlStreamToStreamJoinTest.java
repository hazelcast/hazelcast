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

package com.hazelcast.jet.sql.impl.s2sjoin;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlStreamToStreamJoinTest extends SqlTestSupport {
    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(3, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_missingBound() {
        TestStreamSqlConnector.create(
                sqlService, "s", asList("a", "b"), asList(INTEGER, INTEGER));
        sqlService.execute("CREATE VIEW v AS SELECT * FROM TABLE(IMPOSE_ORDER(TABLE s, DESCRIPTOR(a), 0))");

        // missing both bounds
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM v v1 JOIN v v2 ON 1=2"))
                .hasMessageContaining("A stream-to-stream join must have a join condition constraining the maximum difference " +
                        "between time values of the joined tables in both directions")
                .hasMessageContaining("Time columns on the left side: [a], time columns on the right side: [a]");

        // missing one bound
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM v v1 JOIN v v2 ON v1.a >= v2.a"))
                .hasMessageContaining("A stream-to-stream join must have a join condition constraining the maximum difference " +
                        "between time values of the joined tables in both directions");
        // missing the other bound
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM v v1 JOIN v v2 ON v2.a >= v1.a"))
                .hasMessageContaining("A stream-to-stream join must have a join condition constraining the maximum difference " +
                        "between time values of the joined tables in both directions");

        // bounds in both directions, but not involving a column from left
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM v v1 JOIN v v2 ON v1.a BETWEEN v1.b AND v2.a"))
                .hasMessageContaining("A stream-to-stream join must have a join condition constraining the maximum difference " +
                        "between time values of the joined tables in both directions");

        // bounds in both directions, but not involving a column from right
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM v v1 JOIN v v2 ON v2.a BETWEEN v2.b AND v1.a"))
                .hasMessageContaining("A stream-to-stream join must have a join condition constraining the maximum difference " +
                        "between time values of the joined tables in both directions");
    }

    @Test
    public void test_joinIsContinuous() {
        String stream = "stream1";
        TestStreamSqlConnector.create(
                sqlService,
                stream,
                singletonList("a"),
                singletonList(TIMESTAMP_WITH_TIME_ZONE),
                row(timestampTz(0L)),
                row(timestampTz(1L)),
                row(timestampTz(2L))
        );

        String stream2 = "stream2";
        TestStreamSqlConnector.create(
                sqlService,
                stream2,
                singletonList("b"),
                singletonList(TIMESTAMP_WITH_TIME_ZONE),
                row(timestampTz(1L)),
                row(timestampTz(2L))
        );

        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '0.002' SECOND))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream2, DESCRIPTOR(b), INTERVAL '0.002' SECOND))");

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM s1 JOIN s2 ON s2.b BETWEEN s1.a AND s1.a + INTERVAL '0.002' SECOND ",
                asList(
                        new Row(timestampTz(0L), timestampTz(1L)),
                        new Row(timestampTz(0L), timestampTz(2L)),
                        new Row(timestampTz(1L), timestampTz(1L)),
                        new Row(timestampTz(1L), timestampTz(2L)),
                        new Row(timestampTz(2L), timestampTz(2L))
                )
        );
    }

    @Test
    public void test_leftStreamToStreamJoinWithTimeBounds() {
        String stream = "stream1";
        TestStreamSqlConnector.create(
                sqlService,
                stream,
                singletonList("a"),
                singletonList(TIMESTAMP_WITH_TIME_ZONE),
                row(timestampTz(0L)),
                row(timestampTz(3L)),
                row(timestampTz(4L)),
                row(timestampTz(5L)),
                row(timestampTz(51L)) // flushing event
        );

        String stream2 = "stream2";
        TestStreamSqlConnector.create(
                sqlService,
                stream2,
                singletonList("b"),
                singletonList(TIMESTAMP_WITH_TIME_ZONE),
                row(timestampTz(2L)),
                row(timestampTz(5L)),
                row(timestampTz(7L)),
                row(timestampTz(50L)) // flushing event
        );

        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '0.002' SECOND))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream2, DESCRIPTOR(b), INTERVAL '0.002' SECOND))");

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM s1 LEFT JOIN s2 ON s2.b BETWEEN s1.a AND s1.a + INTERVAL '0.001' SECOND ",
                asList(
                        new Row(timestampTz(0L), null),
                        new Row(timestampTz(3L), null),
                        new Row(timestampTz(4L), timestampTz(5L)),
                        new Row(timestampTz(5L), timestampTz(5L))
                )
        );
    }

    @Test
    public void test_rightStreamToStreamJoinWithTimeBounds() {
        String stream = "stream1";
        TestStreamSqlConnector.create(
                sqlService,
                stream,
                singletonList("a"),
                singletonList(TIMESTAMP_WITH_TIME_ZONE),
                row(timestampTz(2L)),
                row(timestampTz(5L)),
                row(timestampTz(7L)),
                row(timestampTz(50L)) // flushing event
        );

        String stream2 = "stream2";
        TestStreamSqlConnector.create(
                sqlService,
                stream2,
                singletonList("b"),
                singletonList(TIMESTAMP_WITH_TIME_ZONE),
                row(timestampTz(0L)),
                row(timestampTz(3L)),
                row(timestampTz(4L)),
                row(timestampTz(5L)),
                row(timestampTz(51L)) // flushing event
        );

        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '0.002' SECOND))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream2, DESCRIPTOR(b), INTERVAL '0.002' SECOND))");

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM s1 RIGHT JOIN s2 ON s1.a BETWEEN s2.b AND s2.b + INTERVAL '0.001' SECOND ",
                asList(
                        new Row(null, timestampTz(0L)),
                        new Row(null, timestampTz(3L)),
                        new Row(timestampTz(5L), timestampTz(4L)),
                        new Row(timestampTz(5L), timestampTz(5L))
                )
        );
    }

    @Test
    public void test_joinHasTimestampBounds() {
        String stream = "stream1";
        TestStreamSqlConnector.create(
                sqlService,
                stream,
                singletonList("a"),
                singletonList(TIMESTAMP),
                row(timestamp(0L)));

        String stream2 = "stream2";
        TestStreamSqlConnector.create(
                sqlService,
                stream2,
                singletonList("b"),
                singletonList(TIMESTAMP),
                row(timestamp(0L)));

        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '0.001' SECOND))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream2, DESCRIPTOR(b), INTERVAL '0.001' SECOND))");

        assertTipOfStream(
                "SELECT * FROM s1 JOIN s2 ON s2.b BETWEEN s1.a - INTERVAL '0.001' SECOND " +
                        "                             AND     s1.a + INTERVAL '0.004' SECOND ",
                singletonList(new Row(timestamp(0L), timestamp(0L)))
        );
    }

    @Test
    public void test_joinIsEquiJoin() {
        String stream = "stream1";
        TestStreamSqlConnector.create(
                sqlService,
                stream,
                singletonList("a"),
                singletonList(TIMESTAMP),
                row(timestamp(0L)));

        String stream2 = "stream2";
        TestStreamSqlConnector.create(
                sqlService,
                stream2,
                singletonList("b"),
                singletonList(TIMESTAMP),
                row(timestamp(0L)));

        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '0.001' SECOND))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream2, DESCRIPTOR(b), INTERVAL '0.001' SECOND))");

        assertTipOfStream(
                "SELECT * FROM s1 JOIN s2 ON s2.b = s1.a",
                singletonList(new Row(timestamp(0L), timestamp(0L)))
        );
    }

    @Test
    public void when_joinIsEquiJoinBetweenFunctions_then_fail() {
        String stream = "stream1";
        TestStreamSqlConnector.create(
                sqlService,
                stream,
                singletonList("a"),
                singletonList(TIMESTAMP),
                row(timestamp(2L)));

        String stream2 = "stream2";
        TestStreamSqlConnector.create(
                sqlService,
                stream2,
                singletonList("b"),
                singletonList(TIMESTAMP),
                row(timestamp(2L)));

        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '0.001' SECOND))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream2, DESCRIPTOR(b), INTERVAL '0.001' SECOND))");

        // TODO: TO_TIMESTAMP
        assertThatThrownBy(() ->
                        sqlService.execute("SELECT * FROM s1 JOIN s2 ON TO_TIMESTAMP_TZ(s2.b) = TO_TIMESTAMP_TZ(2)"),
                "Only time bound / equality condition are supported for stream-to-stream JOIN"
        );
    }

    @Test
    public void test_additionalJoinConditionApplies() {
        String stream = "stream1";
        TestStreamSqlConnector.create(
                sqlService,
                stream,
                asList("a", "b"),
                asList(TIMESTAMP, INTEGER),
                row(timestamp(9L), 9),
                row(timestamp(11L), 11),
                row(timestamp(12L), 12),
                row(timestamp(13L), 13),
                row(timestamp(14L), 14),
                row(timestamp(15L), 15)
        );

        String stream2 = "stream2";
        TestStreamSqlConnector.create(
                sqlService,
                stream2,
                asList("x", "y"),
                asList(TIMESTAMP, INTEGER),
                row(timestamp(9L), 9),
                row(timestamp(10L), 10),
                row(timestamp(11L), 11),
                row(timestamp(12L), 12),
                row(timestamp(13L), 13),
                row(timestamp(14L), 14),
                row(timestamp(15L), 15)
        );

        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '0.002' SECOND))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream2, DESCRIPTOR(x), INTERVAL '0.002' SECOND))");

        assertRowsEventuallyInAnyOrder(
                "SELECT b, y FROM s1" +
                        " JOIN s2 ON s2.x BETWEEN s1.a AND s1.a + INTERVAL '0.002' SECOND WHERE b % 2 = 0 ",
                asList(
                        new Row(12, 12),
                        new Row(12, 13),
                        new Row(12, 14),
                        new Row(14, 14),
                        new Row(14, 15)
                )
        );
    }

    @Test
    public void test_nonTemporalWatermarkedType() {
        TestStreamSqlConnector.create(sqlService, "stream1", singletonList("a"), singletonList(INTEGER), row(42));
        TestStreamSqlConnector.create(sqlService, "stream2", singletonList("a"), singletonList(INTEGER), row(42));

        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), 1))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream2, DESCRIPTOR(a), 1))");

        assertTipOfStream("SELECT * FROM s1 JOIN s2 ON s1.a=s2.a", singletonList(new Row(42, 42)));
    }

    @Test
    public void test_joinHasDoubledTimestampBounds() {
        String stream1 = "stream1";
        TestStreamSqlConnector.create(
                sqlService,
                stream1,
                singletonList("a"),
                singletonList(TIMESTAMP),
                row(timestamp(100L)));

        String stream2 = "stream2";
        TestStreamSqlConnector.create(
                sqlService,
                stream2,
                singletonList("b"),
                singletonList(TIMESTAMP),
                row(timestamp(100L)));

        String stream3 = "stream3";
        TestStreamSqlConnector.create(
                sqlService,
                stream3,
                singletonList("c"),
                singletonList(TIMESTAMP),
                row(timestamp(101L)));

        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '0.001' SECOND))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream2, DESCRIPTOR(b), INTERVAL '0.001' SECOND))");
        sqlService.execute("CREATE VIEW s3 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream3, DESCRIPTOR(c), INTERVAL '0.001' SECOND))");

        assertTipOfStream(
                "SELECT * FROM s1 " +
                        "JOIN s2 ON s2.b BETWEEN s1.a - INTERVAL '0.1' SECONDS AND s1.a " +
                        "JOIN s3 ON s3.c BETWEEN s2.b - INTERVAL '0.1' SECONDS AND s2.b + INTERVAL '0.5' SECONDS",
                singletonList(new Row(timestamp(100L), timestamp(100L), timestamp(101L)))
        );
    }

    @Test
    public void test_joinHasTripledTimestampBounds() {
        String stream1 = "stream1";
        TestStreamSqlConnector.create(
                sqlService,
                stream1,
                singletonList("a"),
                singletonList(TIMESTAMP),
                row(timestamp(100L)));

        String stream2 = "stream2";
        TestStreamSqlConnector.create(
                sqlService,
                stream2,
                singletonList("b"),
                singletonList(TIMESTAMP),
                row(timestamp(100L)));

        String stream3 = "stream3";
        TestStreamSqlConnector.create(
                sqlService,
                stream3,
                singletonList("c"),
                singletonList(TIMESTAMP),
                row(timestamp(101L)));

        String stream4 = "stream4";
        TestStreamSqlConnector.create(
                sqlService,
                stream4,
                singletonList("d"),
                singletonList(TIMESTAMP),
                row(timestamp(102L)));

        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '0.001' SECOND))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream2, DESCRIPTOR(b), INTERVAL '0.001' SECOND))");
        sqlService.execute("CREATE VIEW s3 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream3, DESCRIPTOR(c), INTERVAL '0.001' SECOND))");
        sqlService.execute("CREATE VIEW s4 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream4, DESCRIPTOR(d), INTERVAL '0.001' SECOND))");


        assertTipOfStream(
                "SELECT * FROM s1 " +
                        "JOIN s2 ON s2.b BETWEEN s1.a - INTERVAL '0.1' SECONDS AND s1.a " +
                        "JOIN s3 ON s3.c BETWEEN s2.b AND s2.b + INTERVAL '0.1' SECONDS " +
                        "JOIN s4 ON s4.d BETWEEN s3.c AND s3.c + INTERVAL '0.1' SECONDS ",
                singletonList(new Row(
                        timestamp(100L),
                        timestamp(100L),
                        timestamp(101L),
                        timestamp(102L)))
        );
    }

    @Test
    public void test_calcReordersFields() {
        String stream = "stream1";
        TestStreamSqlConnector.create(
                sqlService,
                stream,
                singletonList("a"),
                singletonList(TIMESTAMP),
                row(timestamp(0L)));

        String stream2 = "stream2";
        TestStreamSqlConnector.create(
                sqlService,
                stream2,
                asList("b", "c", "d"),
                asList(TIMESTAMP, INTEGER, VARCHAR),
                row(timestamp(0L), 0, "zero"));

        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '0.001' SECOND))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream2, DESCRIPTOR(b), INTERVAL '0.001' SECOND))");

        assertTipOfStream(
                "SELECT d, c, b, a FROM s1 JOIN s2 ON s2.b BETWEEN s1.a - INTERVAL '0.001' SECOND " +
                        "                                      AND     s1.a + INTERVAL '0.004' SECOND ",
                singletonList(new Row("zero", 0, timestamp(0L), timestamp(0L)))
        );
    }

    @Test
    public void test_calcHasParent() {
        String stream = "stream1";
        TestStreamSqlConnector.create(
                sqlService,
                stream,
                singletonList("a"),
                singletonList(TIMESTAMP),
                row(timestamp(0L)));

        String stream2 = "stream2";
        TestStreamSqlConnector.create(
                sqlService,
                stream2,
                asList("b", "c", "d"),
                asList(TIMESTAMP, INTEGER, VARCHAR),
                row(timestamp(0L), 0, "zero"));

        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '0.001' SECOND))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT b FROM TABLE(IMPOSE_ORDER(TABLE stream2, DESCRIPTOR(b), INTERVAL '0.001' SECOND))");

        String join = "(SELECT b, a FROM s1 JOIN s2 ON s2.b BETWEEN s1.a - INTERVAL '0.001' SECOND " +
                "                                           AND     s1.a + INTERVAL '0.004' SECOND)";
        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, window_end, a FROM " +
                        "TABLE(TUMBLE(" + join +
                        "  , DESCRIPTOR(a)" +
                        "  , INTERVAL '0.002' SECOND" +
                        "))",
                singletonList(
                        new Row(timestamp(0L), timestamp(2L), timestamp(0L))
                )
        );
    }

    @Test
    public void test_relTreeHasUnion() {
        String stream = "stream1";
        TestStreamSqlConnector.create(
                sqlService,
                stream,
                singletonList("a"),
                singletonList(TIMESTAMP),
                row(timestamp(0L)),
                row(timestamp(1L)));

        String stream2 = "stream2";
        TestStreamSqlConnector.create(
                sqlService,
                stream2,
                asList("b", "c", "d"),
                asList(TIMESTAMP, INTEGER, VARCHAR),
                row(timestamp(0L), 0, "zero"),
                row(timestamp(1L), 1, "one"));
        String stream3 = "stream3";
        TestStreamSqlConnector.create(
                sqlService,
                stream3,
                asList("b", "c", "d"),
                asList(TIMESTAMP, INTEGER, VARCHAR),
                row(timestamp(1L), 1, "one"),
                row(timestamp(2L), 2, "two"));

        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '0.001' SECOND))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT b, d FROM TABLE(IMPOSE_ORDER(TABLE stream2, DESCRIPTOR(b), INTERVAL '0.001' SECOND)) " +
                "UNION ALL " +
                "SELECT b, d FROM TABLE(IMPOSE_ORDER(TABLE stream3, DESCRIPTOR(b), INTERVAL '0.001' SECOND)) ");

        assertRowsEventuallyInAnyOrder(
                "SELECT a, d FROM s1 JOIN s2 " +
                        "ON s2.b BETWEEN s1.a - INTERVAL '0.001' SECOND " +
                        "        AND     s1.a + INTERVAL '0.004' SECOND ",
                asList(
                        new Row(timestamp(0L), "zero"),
                        new Row(timestamp(0L), "one"),
                        new Row(timestamp(0L), "one"),
                        new Row(timestamp(0L), "two"),
                        new Row(timestamp(1L), "zero"),
                        new Row(timestamp(1L), "one"),
                        new Row(timestamp(1L), "one"),
                        new Row(timestamp(1L), "two")
                )
        );
    }

    @Test
    public void test_selfJoin() {
        TestStreamSqlConnector.create(
                sqlService,
                "stream1",
                singletonList("a"),
                singletonList(TIMESTAMP_WITH_TIME_ZONE),
                row(timestampTz(42L)));

        sqlService.execute("CREATE VIEW s AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '0' SECONDS))");

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM s s1 JOIN s s2 ON s1.a=s2.a",
                singletonList(new Row(timestampTz(42L), timestampTz(42L))));
    }

    @Test
    public void test_joinWithoutViews() {
        TestStreamSqlConnector.create(
                sqlService,
                "stream1",
                singletonList("a"),
                singletonList(TIMESTAMP_WITH_TIME_ZONE),
                row(timestampTz(42L)));

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " +
                        "(SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '1' SECONDS))) s1 " +
                        "INNER JOIN " +
                        "(SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '1' SECONDS))) s2 " +
                        "ON s1.a=s2.a",
                singletonList(new Row(timestampTz(42L), timestampTz(42L))));
    }

    @Test
    public void test_joinWithoutViewsAndSubqueries() {
        TestStreamSqlConnector.create(
                sqlService,
                "stream1",
                singletonList("a"),
                singletonList(TIMESTAMP_WITH_TIME_ZONE),
                row(timestampTz(42L)));

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " +
                        "TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '1' SECONDS)) s1 " +
                        "INNER JOIN " +
                        "TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '1' SECONDS)) s2 " +
                        "ON s1.a=s2.a",
                singletonList(new Row(timestampTz(42L), timestampTz(42L))));
    }

    @Test
    public void test_joinWithUsingClause() {
        TestStreamSqlConnector.create(
                sqlService,
                "stream1",
                singletonList("a"),
                singletonList(TIMESTAMP_WITH_TIME_ZONE),
                row(timestampTz(42L)));

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " +
                        "(SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '1' SECONDS))) s1 " +
                        "INNER JOIN " +
                        "(SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), INTERVAL '1' SECONDS))) s2 " +
                        " USING(a)",
                singletonList(new Row(timestampTz(42L))));
    }

    @Test
    public void test_joinGenerators() {
        assertThatThrownBy(() ->
                sqlService.execute("SELECT 1 from TABLE(GENERATE_STREAM(1)) JOIN TABLE(GENERATE_STREAM(3)) on 1=1;"))
                .hasMessageContaining("For stream-to-stream join, both joined sides must have an order imposed");
    }

    @Test
    public void test_joinWithImplicitCastsInJoinCondition() {
        String stream = "stream1";
        TestStreamSqlConnector.create(
                sqlService,
                stream,
                singletonList("a"),
                singletonList(INTEGER),
                row(0));

        String stream2 = "stream2";
        TestStreamSqlConnector.create(
                sqlService,
                stream2,
                singletonList("b"),
                singletonList(INTEGER),
                row(0));

        sqlService.execute("CREATE VIEW s1 AS SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), 1))");
        sqlService.execute("CREATE VIEW s2 AS SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream2, DESCRIPTOR(b), 1))");

        assertTipOfStream(
                "SELECT * FROM s1 JOIN s2 ON s2.b BETWEEN s1.a AND s1.a + 1",
                singletonList(new Row(0, 0)));
    }

    @Test
    public void test_joinWithUnsupportedCastsInJoinCondition() {
        String stream = "stream1";
        TestStreamSqlConnector.create(
                sqlService,
                stream,
                singletonList("a"),
                singletonList(INTEGER),
                row(0));

        String stream2 = "stream2";
        TestStreamSqlConnector.create(
                sqlService,
                stream2,
                singletonList("b"),
                singletonList(INTEGER),
                row(0));

        sqlService.execute("CREATE VIEW s1 AS SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(a), 1))");
        sqlService.execute("CREATE VIEW s2 AS SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream2, DESCRIPTOR(b), 1))");

        assertThatThrownBy(() ->
                sqlService.execute("SELECT * FROM s1 JOIN s2 ON s2.b " +
                        "BETWEEN s1.a AND CAST(CAST(s1.a as VARCHAR) AS TIMESTAMP) + INTERVAL '10' SECONDS"))
                .hasMessageContaining("A stream-to-stream join must have a join condition constraining");
    }
}
