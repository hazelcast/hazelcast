/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.aggregation;

import com.hazelcast.aggregation.impl.DoubleAverageAggregator;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapAggregatePerformanceTest extends HazelcastTestSupport {

    @Test
    @Ignore("needs 10G of heap to run")
    public void doubleAvg_10millionValues_1node_primitiveValue() {
        IMap<Long, Double> map = getMapWithNodeCount(1);
        //map.addIndex("__VALUE", true);

        System.err.println("Initialising");

        int elementCount = 10000000;
        double value = 0;
        Map<Long, Double> values = new HashMap<Long, Double>(elementCount);
        for (long i = 0L; i < elementCount; i++) {
            values.put(i, value++);
        }

        System.err.println("Putting");
        long putStart = System.currentTimeMillis();
        map.putAll(values);
        long putStop = System.currentTimeMillis();
        System.err.println("Finished putting " + (putStop - putStart) + " millis");

        System.err.println("Executing bare metal");
        long start1 = System.currentTimeMillis();

        int count = 0;
        double sum = 0d;
        for (Double d : values.values()) {
            sum += d;
            count++;
        }
        Double avg1 = sum / ((double) count);
        long stop1 = System.currentTimeMillis();
        System.err.println("Finished avg in " + (stop1 - start1) + " millis avg=" + avg1);

        for (int i = 0; i < 10; i++) {
            System.gc();
        }

        for (int i = 0; i < 10; i++) {
            System.err.println("Executing aggregation");
            long start = System.currentTimeMillis();
            Double avg = map.aggregate(new DoubleAverageAggregator<Map.Entry<Long, Double>>());
            long stop = System.currentTimeMillis();
            System.err.println("\nFinished avg in " + (stop - start) + " millis avg=" + avg);
            System.err.println("------------------------------------------");
        }
    }

    @Test
    @Ignore("needs 10G of heap to run")
    public void doubleAvg_10millionValues_1node_objectValue() {
        IMap<Long, Person> map = getMapWithNodeCount(1);
        //map.addIndex("age", true);

        System.err.println("Initialising");

        int elementCount = 10000000;
        double value = 0;
        Map<Long, Person> values = new HashMap<Long, Person>(elementCount);
        for (long i = 0L; i < elementCount; i++) {
            values.put(i, new Person(value++));
        }

        System.err.println("Putting");
        long putStart = System.currentTimeMillis();
        map.putAll(values);
        long putStop = System.currentTimeMillis();
        System.err.println("Finished putting " + (putStop - putStart) + " millis");

        System.err.println("Executing bare metal");
        long start1 = System.currentTimeMillis();

        int count = 0;
        double sum = 0d;
        for (Person p : values.values()) {
            sum += p.age;
            count++;
        }
        Double avg1 = sum / ((double) count);
        long stop1 = System.currentTimeMillis();
        System.err.println("Finished avg in " + (stop1 - start1) + " millis avg=" + avg1);

        for (int i = 0; i < 10; i++) {
            System.gc();
        }

        for (int i = 0; i < 10; i++) {
            System.err.println("Executing aggregation");
            long start = System.currentTimeMillis();
            Double avg = map.aggregate(new DoubleAverageAggregator<Map.Entry<Long, Person>>("age"));
            long stop = System.currentTimeMillis();
            System.err.println("\nFinished avg in " + (stop - start) + " millis avg=" + avg);
            System.err.println("------------------------------------------");
        }
    }

    private <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count < 1");
        }

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);

        Config config = new Config();
        //config.setProperty("hazelcast.partition.count", "3");
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("aggr");
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        mapConfig.setBackupCount(0);
        config.addMapConfig(mapConfig);

        config.setProperty("hazelcast.query.predicate.parallel.evaluation", "true");

        HazelcastInstance instance = factory.newInstances(config)[0];
        return instance.getMap("aggr");
    }
}
