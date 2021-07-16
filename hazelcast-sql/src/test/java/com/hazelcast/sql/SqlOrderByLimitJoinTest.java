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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class SqlOrderByLimitJoinTest extends SqlTestSupport {
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
            t1.put(i, "test-" + i);
        }
    }

    @Test
    public void when_orderByWithLimitUsedOnJoinedTable_limitWorks() {
        final List<Map<String, Object>> results = execute("select abs(t0.this) as c1, t1.this as c2 "
                + "from t0 join t1 on t0.__key = t1.__key "
                + "order by t0.this desc "
                + "limit 5");

        final List<Object> c1s = results.stream().map(m -> m.get("c1")).collect(Collectors.toList());

        assertEquals(5, results.size());
        assertEquals(Arrays.asList(10L, 9L, 8L, 7L, 6L), c1s);
    }

    @Test
    public void when_orderByWithLimitAndOffsetUsedOnJoinedTable_limitAndOffsetWork() {
        final List<Map<String, Object>> results = execute("select abs(t0.this) as c1, t1.this as c2 "
                + "from t0 join t1 on t0.__key = t1.__key "
                + "order by t0.this desc "
                + "limit 5 offset 2");

        final List<Object> c1s = results.stream().map(m -> m.get("c1")).collect(Collectors.toList());

        assertEquals(5, results.size());
        assertEquals(Arrays.asList(8L, 7L, 6L, 5L, 4L), c1s);
    }

    private List<Map<String, Object>> execute(String sql) {
        final List<Map<String, Object>> results = new ArrayList<>();
        for (final SqlRow row : instance().getSql().execute(sql)) {
            final Map<String, Object> result = new HashMap<>();
            for (final SqlColumnMetadata column : row.getMetadata().getColumns()) {
                final String key = column.getName();
                final Object value = row.getObject(key);
                result.put(key, value);
            }
            results.add(result);
        }

        return results;
    }

}
