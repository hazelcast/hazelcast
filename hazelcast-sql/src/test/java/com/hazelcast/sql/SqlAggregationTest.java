/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlAggregationTest extends HazelcastTestSupport {
    private HazelcastInstance member;

    @Before
    public void before() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        Config cfg = new Config();

        member = nodeFactory.newHazelcastInstance(cfg);
        nodeFactory.newHazelcastInstance(cfg);

        IMap<Long, Value> map = member.getMap("map");

        for (int i = 0; i < 1000; i++)
            map.put((long)i, new Value(i % 100, i % 10, i ));

        ReplicatedMap<Long, Value> replicatedMap = member.getReplicatedMap("rmap");

        for (int i = 0; i < 1000; i++)
            replicatedMap.put((long)i, new Value(i % 100, i % 10, i ));
    }

    @After
    public void after() {
        member = null;

        Hazelcast.shutdownAll();
    }

    @Test(timeout = Long.MAX_VALUE)
    public void testNoGroupBy() throws Exception {
        doQuery("SELECT SUM(v1), SUM(v3) FROM map");
    }

    @Test(timeout = Long.MAX_VALUE)
    public void testGroupBy1() throws Exception {
        doQuery("SELECT SUM(v2), SUM(v3) FROM rmap GROUP BY v1");
    }

    @Test(timeout = Long.MAX_VALUE)
    public void testGroupBy2() throws Exception {
        doQuery("SELECT v1, v2, SUM(v3) FROM map GROUP BY v1, v2");
    }

    @Test(timeout = Long.MAX_VALUE)
    public void testGroupByHaving() throws Exception {
        doQuery("SELECT SUM(v1) FROM map GROUP BY v2 HAVING SUM(v3) = 10");
    }

    @Test(timeout = Long.MAX_VALUE)
    public void testGroupByGroupingSet() throws Exception {
        doQuery("SELECT v1, v2, SUM(v3) + SUM(v3) FROM map GROUP BY GROUPING SETS ((v1, v2), (v2))");
    }

    // TODO: Remove
    @Test(timeout = Long.MAX_VALUE)
    public void testSort() throws Exception {
        doQuery("SELECT v2 * 2, v2 FROM rmap");
    }

    private List<SqlRow> doQuery(String sql) {
        SqlCursor cursor = member.getSqlService().query(sql);

        List<SqlRow> res = new ArrayList<>();

        for (SqlRow row : cursor)
            res.add(row);

        return res;
    }

    private static class Value implements Serializable {
        private static final long serialVersionUID = 4174838810062826615L;

        private int v1;
        private int v2;
        private int v3;

        public Value() {
            // No-op.
        }

        public Value(int v1, int v2, int v3) {
            this.v1 = v1;
            this.v2 = v2;
            this.v3 = v3;
        }
    }
}
