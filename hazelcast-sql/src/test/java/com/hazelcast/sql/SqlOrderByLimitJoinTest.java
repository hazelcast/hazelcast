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
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SqlOrderByLimitJoinTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        initialize(1, new Config().setJetConfig(new JetConfig().setEnabled(true)));
    }

    @Test
    public void when_orderByWithLimitUsedOnJoinedTable_limitWorks() {
        final IMap<Long, Long> t0 = instance().getMap("t0");
        final IMap<Long, String> t1 = instance().getMap("t1");

        for (long i = 0; i < 10; i++) {
            t0.put(i, i * 1000);
            t1.put(i, "test-" + i);
        }

        final List<Map<String, Object>> results = execute("select abs(t0.this) as c1, t1.this as c2 "
                + "from t0 join t1 on t0.__key = t1.__key "
                + "order by t0.this "
                + "limit 5");

        assertEquals(5, results.size());
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
