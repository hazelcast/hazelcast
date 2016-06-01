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
package com.hazelcast.jet.stream.benchmark;

import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.jet.stream.JetStreamTestSupport;
import com.hazelcast.mapreduce.aggregation.Aggregations;
import com.hazelcast.mapreduce.aggregation.PropertyExtractor;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Map;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
@Ignore
public class AggregatorTest extends JetStreamTestSupport implements Serializable {

    private static final int COUNT = 100_000;
    private static IStreamMap<String, Integer> map;

    @BeforeClass
    public static void setUp() {
        map = getMap(instance);
        fillMap(map, COUNT);
    }

    @Test @Ignore
    public void testAggregator() throws Exception {
        long start = System.currentTimeMillis();
        long sum = map.aggregate(Supplier.all(new PropertyExtractor<Integer, Long>() {
            @Override
            public Long extract(Integer integer) {
                return (long)integer;
            }
        }), Aggregations.longSum());
        System.out.println("aggregations: sum=" + sum + " totalTime=" + (System.currentTimeMillis() - start));
    }

    @Test
    public void testStream() throws Exception {
        long start = System.currentTimeMillis();
        long sum = map.stream().mapToLong(Map.Entry::getValue).sum();
        System.out.println("java.util.stream: sum=" + sum + " totalTime=" + (System.currentTimeMillis() - start));
    }
}
