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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE;
import static java.util.Arrays.asList;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SqlStreamingJoinAndAggregationTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Test
    public void test_singleStaged_hop() {
        String stream1 = "stream1";

        TestStreamSqlConnector.create(
                instance().getSql(),
                stream1,
                asList("a", "b"),
                asList(INTEGER, TIMESTAMP_WITH_TIME_ZONE),
                row(1, timestampTz(1L)),
                row(2, timestampTz(2L)),
                row(101, timestampTz(101L)) // flushing event
        );

        String stream2 = "stream_a";
        TestStreamSqlConnector.create(
                instance().getSql(),
                stream2,
                asList("c", "d"),
                asList(TIMESTAMP_WITH_TIME_ZONE, INTEGER),
                row(timestampTz(1L), 1),
                row(timestampTz(2L), 2),
                row(timestampTz(101L), 101) // flushing event
        );

        instance().getSql().execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(b), INTERVAL '0.001' SECOND))");
        instance().getSql().execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream_a, DESCRIPTOR(c), INTERVAL '0.001' SECOND))");

        String sql = "SELECT we1, _sum, _max FROM " +
                "( SELECT window_end AS we1, SUM(DISTINCT a) AS _sum FROM " +
                "    TABLE(HOP(TABLE s1, DESCRIPTOR(b), INTERVAL '0.02' SECOND, INTERVAL '0.01' SECOND))" +
                "    GROUP BY window_end) st1" +
                "  JOIN " +
                "( SELECT window_end AS we2, MAX(DISTINCT d) AS _max FROM " +
                "    TABLE(HOP(TABLE s2, DESCRIPTOR(c), INTERVAL '0.02' SECOND, INTERVAL '0.01' SECOND))" +
                "    GROUP BY window_end) st2" +
                " ON st1.we1 = st2.we2";

        assertRowsEventuallyInAnyOrder(sql, asList(
                new Row(timestampTz(10L), 3L, 2),
                new Row(timestampTz(20L), 3L, 2)
        ));
    }

    @Test
    public void test_singleStaged_tumble() {
        String stream1 = "stream1";

        TestStreamSqlConnector.create(
                instance().getSql(),
                stream1,
                asList("a", "b"),
                asList(INTEGER, TIMESTAMP_WITH_TIME_ZONE),
                row(1, timestampTz(1L)),
                row(3, timestampTz(3L)),
                row(5, timestampTz(5L)),
                row(5, timestampTz(7L)),
                row(41, timestampTz(41L)) // flushing event
        );

        String stream2 = "stream_a";
        TestStreamSqlConnector.create(
                instance().getSql(),
                stream2,
                asList("c", "d"),
                asList(TIMESTAMP_WITH_TIME_ZONE, INTEGER),
                row(timestampTz(0L), 0),
                row(timestampTz(2L), 2),
                row(timestampTz(4L), 4),
                row(timestampTz(6L), 6),
                row(timestampTz(41L), 41) // flushing event
        );

        instance().getSql().execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(b), INTERVAL '0.003' SECOND))");
        instance().getSql().execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream_a, DESCRIPTOR(c), INTERVAL '0.003' SECOND))");

        String sql = "SELECT we1, sum1, max2 FROM " +
                "( SELECT window_end AS we1, SUM(DISTINCT a) AS sum1 FROM " +
                "    TABLE(TUMBLE(TABLE s1, DESCRIPTOR(b), INTERVAL '0.003' SECOND))" +
                "    GROUP BY window_end) st1" +
                "  JOIN " +
                "( SELECT window_end AS we2, MAX(DISTINCT d) AS max2 FROM " +
                "    TABLE(TUMBLE(TABLE s2, DESCRIPTOR(c), INTERVAL '0.003' SECOND))" +
                "    GROUP BY window_end) st2" +
                " ON st1.we1 = st2.we2";

        assertRowsEventuallyInAnyOrder(sql,
                asList(
                        new Row(timestampTz(3), 1L, 2),
                        new Row(timestampTz(6), 8L, 4),
                        new Row(timestampTz(9), 5L, 6))
        );
    }

    @Test
    public void test_doubleStaged_hop() {
        String stream1 = "stream1x";

        TestStreamSqlConnector.create(
                instance().getSql(),
                stream1,
                asList("a", "b"),
                asList(INTEGER, TIMESTAMP_WITH_TIME_ZONE),
                row(1, timestampTz(1L)),
                row(2, timestampTz(2L)),
                row(101, timestampTz(101L)) // flushing event
        );

        String stream2 = "stream_ax";
        TestStreamSqlConnector.create(
                instance().getSql(),
                stream2,
                asList("c", "d"),
                asList(TIMESTAMP_WITH_TIME_ZONE, INTEGER),
                row(timestampTz(1L), 1),
                row(timestampTz(2L), 2),
                row(timestampTz(101L), 101) // flushing event
        );

        instance().getSql().execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1x, DESCRIPTOR(b), INTERVAL '0.001' SECOND))");
        instance().getSql().execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream_ax, DESCRIPTOR(c), INTERVAL '0.001' SECOND))");

        String sql = "SELECT we1, _sum, _max FROM " +
                "( SELECT window_end AS we1, SUM(a) AS _sum FROM " +
                "    TABLE(HOP(TABLE s1, DESCRIPTOR(b), INTERVAL '0.02' SECOND, INTERVAL '0.01' SECOND))" +
                "    GROUP BY window_end) st1" +
                "  JOIN " +
                "( SELECT window_end AS we2, MAX(d) AS _max FROM " +
                "    TABLE(HOP(TABLE s2, DESCRIPTOR(c), INTERVAL '0.02' SECOND, INTERVAL '0.01' SECOND))" +
                "    GROUP BY window_end) st2" +
                " ON st1.we1 = st2.we2";

        assertRowsEventuallyInAnyOrder(sql, asList(
                new Row(timestampTz(10L), 3L, 2),
                new Row(timestampTz(20L), 3L, 2)
        ));
    }

    @Test
    public void test_doubleStaged_tumble() {
        String stream1 = "stream11";

        TestStreamSqlConnector.create(
                instance().getSql(),
                stream1,
                asList("a", "b"),
                asList(INTEGER, TIMESTAMP_WITH_TIME_ZONE),
                row(1, timestampTz(1L)),
                row(3, timestampTz(3L)),
                row(5, timestampTz(5L)),
                row(5, timestampTz(7L)),
                row(41, timestampTz(41L)) // flushing event
        );

        String stream2 = "stream_aa";
        TestStreamSqlConnector.create(
                instance().getSql(),
                stream2,
                asList("c", "d"),
                asList(TIMESTAMP_WITH_TIME_ZONE, INTEGER),
                row(timestampTz(0L), 0),
                row(timestampTz(2L), 2),
                row(timestampTz(4L), 4),
                row(timestampTz(6L), 6),
                row(timestampTz(41L), 41) // flushing event
        );

        instance().getSql().execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream11, DESCRIPTOR(b), INTERVAL '0.003' SECOND))");
        instance().getSql().execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream_aa, DESCRIPTOR(c), INTERVAL '0.003' SECOND))");

        String sql = "SELECT we1, sum1, max2 FROM " +
                "( SELECT window_end AS we1, SUM(a) AS sum1 FROM " +
                "    TABLE(TUMBLE(TABLE s1, DESCRIPTOR(b), INTERVAL '0.003' SECOND))" +
                "    GROUP BY window_end) st1" +
                "  JOIN " +
                "( SELECT window_end AS we2, MAX(d) AS max2 FROM " +
                "    TABLE(TUMBLE(TABLE s2, DESCRIPTOR(c), INTERVAL '0.003' SECOND))" +
                "    GROUP BY window_end) st2" +
                " ON st1.we1 = st2.we2";

        assertRowsEventuallyInAnyOrder(sql,
                asList(
                        new Row(timestampTz(3), 1L, 2),
                        new Row(timestampTz(6), 8L, 4),
                        new Row(timestampTz(9), 5L, 6))
        );
    }
}
