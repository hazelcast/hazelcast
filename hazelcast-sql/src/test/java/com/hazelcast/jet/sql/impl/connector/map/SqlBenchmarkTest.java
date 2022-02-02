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

package com.hazelcast.jet.sql.impl.connector.map;

import com.google.common.base.Stopwatch;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlBenchmarkTest extends SqlTestSupport {
    private static final String name = randomName();

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    @Before
    public void setupTest() {
        HazelcastInstance hazelcastInstance = instance();
        createMapping(name, int.class, IdentifiedDataWithLongPortablePojo.class);
        IMap<Integer, IdentifiedDataWithLongPortablePojo> map = hazelcastInstance.getMap(name);
        fillIMapAndGetData(map, 10_00_000);
    }

    @Test
    public void test_basicSelect() {
        for (int i = 0; i < 100; i++) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            SqlStatement statement = new SqlStatement("SELECT sum(\"value\") FROM " + name);
            SqlService sqlService = instance().getSql();
            try (SqlResult result = sqlService.execute(statement)) {
                result.iterator().forEachRemaining(row -> sink(row));
            }
            stopwatch.stop();
            logger.log(Level.INFO, "SQL Result in: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    void sink(Object o) {
    }

//    @Test
//    public void test_basicPredicate() {
//        for (int i = 0; i < 100; i++) {
//            Stopwatch stopwatch = Stopwatch.createStarted();
//            instance().getMap(name).aggregate(Aggregators.longSum("value"));
//            stopwatch.stop();
//            logger.log(Level.INFO, "Predicate Result in: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
//        }
//    }

    private static void fillIMapAndGetData(IMap<Integer, IdentifiedDataWithLongPortablePojo> map, final int count) {
        assert count >= 0;
        List<Row> rows = new ArrayList<>(count);
        int i;
        for (i = 0; i < count; ++i) {
            map.put(i, new IdentifiedDataWithLongPortablePojo(new Integer[10], (long) i));
        }
        assertEquals(i, count);
    }
}
