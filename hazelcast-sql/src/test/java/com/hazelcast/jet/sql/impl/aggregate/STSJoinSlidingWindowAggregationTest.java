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
import com.hazelcast.jet.sql.impl.opt.physical.CreateTopLevelDagVisitor;
import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowAggregatePhysicalRel;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE;
import static java.util.Arrays.asList;


/**
 * It tests a case, when the query has multiple {@link SlidingWindowAggregatePhysicalRel},
 * and one of them has stream to stream JOIN as an input.
 * To be precise, it tests this piece of code :
 * <pre>
 *      byte watermarkKey = watermarkedFieldsKeys.isEmpty()
 *           ? watermarkKeysAssigner.getInputWatermarkKey(rel)  // <- this line.
 *           : watermarkedFieldsKeys.get(rel.timestampField()).getValue();
 * </pre>
 * in {@link CreateTopLevelDagVisitor#onSlidingWindowAggregate(SlidingWindowAggregatePhysicalRel)}
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class STSJoinSlidingWindowAggregationTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Test
    public void test() {
        String stream1 = "stream_a";

        TestStreamSqlConnector.create(
                instance().getSql(),
                stream1,
                asList("a", "b"),
                asList(INTEGER, TIMESTAMP_WITH_TIME_ZONE),
                row(1, timestampTz(1L)),
                row(3, timestampTz(3L)),
                row(5, timestampTz(5L)),
                row(7, timestampTz(7L)),
                row(41, timestampTz(41L)) // flushing event
        );

        String stream2 = "stream_b";
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

        // S2S JOIN Left input view : sliding window SUM aggregation
        instance().getSql().execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream_a, DESCRIPTOR(b), INTERVAL '0.003' SECOND))");
        instance().getSql().execute("CREATE VIEW s_agg1 AS " +
                " SELECT window_end AS we1, SUM(a) AS max2 FROM " +
                " TABLE(TUMBLE(TABLE s1, DESCRIPTOR(b), INTERVAL '0.003' SECOND)) GROUP BY window_end");

        // S2S JOIN right input view : sliding window MAX aggregation
        instance().getSql().execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream_b, DESCRIPTOR(c), INTERVAL '0.003' SECOND))");
        instance().getSql().execute("CREATE VIEW s_agg2 AS " +
                " SELECT window_end AS we2, MAX(d) AS max2 FROM " +
                " TABLE(TUMBLE(TABLE s2, DESCRIPTOR(c), INTERVAL '0.003' SECOND)) GROUP BY window_end");

        // S2S JOIN view
        instance().getSql().execute("CREATE VIEW joined_aggregated_streams AS " +
                "SELECT * FROM s_agg1 JOIN s_agg2 ON s_agg1.we1 = s_agg2.we2"
        );

        String sql = " SELECT window_start AS ws, COUNT(max2) AS cnt FROM " +
                " TABLE(TUMBLE(TABLE joined_aggregated_streams, DESCRIPTOR(we1), INTERVAL '0.003' SECOND)) " +
                " GROUP BY window_start";

        assertRowsEventuallyInAnyOrder(sql,
                asList(
                        new Row(timestampTz(3L), 1L),
                        new Row(timestampTz(6L), 1L),
                        new Row(timestampTz(9L), 1L)
                ));
    }
}
