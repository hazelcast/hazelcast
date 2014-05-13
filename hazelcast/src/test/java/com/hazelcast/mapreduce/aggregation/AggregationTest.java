/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.aggregation;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AggregationTest
        extends HazelcastTestSupport {

    private HazelcastInstance hz1;
    private HazelcastInstance hz2;
    private HazelcastInstance hz3;

    @Before
    public void startup() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        hz1 = nodeFactory.newHazelcastInstance();
        hz2 = nodeFactory.newHazelcastInstance();
        hz3 = nodeFactory.newHazelcastInstance();
    }

    @Test
    public void testIntegerSum()
            throws Exception {

        String mapName = randomMapName();
        IMap<String, Integer> map = hz1.getMap(mapName);

        int expectation = 0;
        for (int i = 0; i < 100000; i++) {
            map.put("key-" + i, i);
            expectation += i;
        }

        Supplier<String, Integer, Integer> supplier = Supplier.all();
        Aggregation<String, Integer, String, Integer, Integer> aggregation = Aggregations.integerSum();
        int sum = map.aggregate(supplier, aggregation);

        assertEquals(expectation, sum);
    }

    @Test
    public void testIntegerSumWithExtractor()
            throws Exception {

        String mapName = randomMapName();
        IMap<String, IntegerValue> map = hz1.getMap(mapName);

        int expectation = 0;
        for (int i = 0; i < 100000; i++) {
            map.put("key-" + i, new IntegerValue(i));
            expectation += i;
        }

        Supplier<String, IntegerValue, Integer> supplier = Supplier.all(new IntegerValuePropertyExtractor());
        Aggregation<String, IntegerValue, String, Integer, Integer> aggregation = Aggregations.integerSum();
        int sum = map.aggregate(supplier, aggregation);

        assertEquals(expectation, sum);
    }

    public static class IntegerValuePropertyExtractor
            implements PropertyExtractor<IntegerValue, Integer>, Serializable {

        @Override
        public Integer extract(IntegerValue value) {
            return value.value;
        }
    }

    public static class IntegerValue
            implements Serializable {

        private int value;

        public IntegerValue() {
        }

        public IntegerValue(int value) {
            this.value = value;
        }
    }
}
