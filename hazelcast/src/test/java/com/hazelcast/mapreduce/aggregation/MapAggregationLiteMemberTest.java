/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapAggregationLiteMemberTest
        extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    private HazelcastInstance instance;

    private HazelcastInstance lite;

    @Before
    public void before() {
        factory = createHazelcastInstanceFactory(4);
        instance = factory.newHazelcastInstance();
        final HazelcastInstance instance2 = factory.newHazelcastInstance();
        lite = factory.newHazelcastInstance(new Config().setLiteMember(true));
        final HazelcastInstance lite2 = factory.newHazelcastInstance(new Config().setLiteMember(true));

        assertClusterSize(4, instance, lite2);
        assertClusterSizeEventually(4, instance2, lite);
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test(timeout = 60000)
    public void testMaxAggregation_fromLite() {
        testMaxAggregation(lite);
    }

    @Test(timeout = 60000)
    public void testMaxAggregation() {
        testMaxAggregation(instance);
    }

    public static void testMaxAggregation(final HazelcastInstance instance) {
        final int size = 2000;
        List<Integer> numbers = new ArrayList<Integer>(size);
        for (int i = 0; i < size; i++) {
            numbers.add(i);
        }

        Collections.shuffle(numbers);
        numbers = numbers.subList(0, 1000);
        final Integer expected = Collections.max(numbers);

        final IMap<Integer, Integer> map = instance.getMap(randomMapName());
        for (Integer number : numbers) {
            map.put(number, number);
        }

        final Aggregation<Integer, Integer, Integer> maxAggregation = Aggregations.integerMax();
        final Integer max = map.aggregate(new ValueSupplier(), maxAggregation);

        assertEquals(expected, max);
    }

    public static class ValueSupplier extends Supplier<Integer, Integer, Integer> implements Serializable {

        @Override
        public Integer apply(Map.Entry<Integer, Integer> entry) {
            return entry.getValue();
        }
    }

}
