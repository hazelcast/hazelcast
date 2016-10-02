/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.mapreduce.aggregation;

import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Aggregations;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@Ignore //https://github.com/hazelcast/hazelcast/issues/5916
public class BaseAggregationMultiMapTest
        extends AbstractAggregationTest {

    @Test
    public void testCountAggregation()
            throws Exception {

        String mapName = randomMapName();
        MultiMap<String, Integer> map = client.getMultiMap(mapName);

        Integer[] values = buildPlainValues(new ValueProvider<Integer>() {
            @Override
            public Integer provideRandom(Random random) {
                return random(1000, 2000);
            }
        }, Integer.class);

        for (int i = 0; i < values.length; i++) {
            map.put("key-" + (i/2), values[i]);
        }

        Supplier<String, Integer, Object> supplier = Supplier.all();
        Aggregation<String, Object, Long> aggregation = Aggregations.count();
        long count = map.aggregate(supplier, aggregation);
        assertEquals(values.length, count);
    }

    @Test
    public void testDistinctValuesAggregation()
            throws Exception {

        final String[] probes = {"Dog", "Food", "Champion", "Hazelcast", "Security", "Integer", "Random", "System"};
        Set<String> expectation = new HashSet<String>(Arrays.asList(probes));

        String mapName = randomMapName();
        MultiMap<String, String> map = client.getMultiMap(mapName);

        String[] values = buildPlainValues(new ValueProvider<String>() {
            @Override
            public String provideRandom(Random random) {
                int index = random.nextInt(probes.length);
                return probes[index];
            }
        }, String.class);

        for (int i = 0; i < values.length; i++) {
            map.put("key-" + (i/2), values[i]);
        }

        Supplier<String, String, String> supplier = Supplier.all();
        Aggregation<String, String, Set<String>> aggregation = Aggregations.distinctValues();
        Set<String> distinctValues = map.aggregate(supplier, aggregation);
        assertEquals(expectation, distinctValues);

    }
}
