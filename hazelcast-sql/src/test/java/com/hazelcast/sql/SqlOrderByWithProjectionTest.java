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

package com.hazelcast.sql;

import com.hazelcast.config.Config;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqlOrderByWithProjectionTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        initialize(1, new Config().setJetConfig(new JetConfig().setEnabled(true)));
    }

    @Before
    public void before() {
        final IMap<Long, Long> t0 = instance().getMap("t0");
        final IMap<Long, String> t1 = instance().getMap("t1");

        for (long i = 0; i < 10; i++) {
            t0.put(i, (i + 1));
            t1.put(i, "test-" + (i + 1));
        }
    }

    @Test
    public void when_orderByWithLimitUsedOnJoinedTable_limitWorks() {
        final String sql = "SELECT ABS(t0.this) AS c1, t1.this AS c2 "
                + "FROM t0 JOIN t1 ON t0.__key = t1.__key "
                + "ORDER BY t0.this DESC "
                + "LIMIT 5";

        assertRowsOrdered(sql, rows(10L, 9L, 8L, 7L, 6L));
    }

    @Test
    public void when_orderByWithLimitAndOffsetUsedOnJoinedTable_limitAndOffsetWork() {
        final String sql = "SELECT ABS(t0.this) AS c1, t1.this AS c2 "
                + "FROM t0 JOIN t1 ON t0.__key = t1.__key "
                + "ORDER BY t0.this DESC "
                + "LIMIT 5 OFFSET 2";

        assertRowsOrdered(sql, rows(8L, 7L, 6L, 5L, 4L));
    }

    private List<Row> rows(Long ...rows) {
        return Stream.of(rows)
                .map(i -> new Row(i, "test-" + i))
                .collect(Collectors.toList());
    }
}
