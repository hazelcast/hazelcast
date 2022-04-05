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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.query.Predicates.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapAggregateTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test(expected = NullPointerException.class)
    public void null_aggregator() {
        getMapWithNodeCount(1).aggregate(null);
    }

    @Test(expected = NullPointerException.class)
    public void null_predicate() {
        getMapWithNodeCount(1).aggregate(new DoubleAverageAggregator(), null);
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("RedundantCast")
    public void null_aggregator_and_predicate() {
        getMapWithNodeCount(1).aggregate((Aggregator) null, (Predicate) null);
    }

    @Test(expected = IllegalArgumentException.class)
    @SuppressWarnings("RedundantCast")
    public void pagingPredicate_fails() {
        getMapWithNodeCount(1).aggregate(new DoubleAverageAggregator(), Predicates.pagingPredicate(1));
    }

    @Test
    public void doubleAvg_1Node_primitiveValue() {
        IMap<String, Double> map = getMapWithNodeCount(1);
        populateMap(map);

        Double avg = map.aggregate(new DoubleAverageAggregator<Map.Entry<String, Double>>());
        assertEquals(Double.valueOf(4.0d), avg);
    }

    @Test
    public void doubleAvg_3Nodes_primitiveValue() {
        IMap<String, Double> map = getMapWithNodeCount(3);
        populateMap(map);

        Double avg = map.aggregate(new DoubleAverageAggregator<Map.Entry<String, Double>>());
        assertEquals(Double.valueOf(4.0d), avg);
    }

    @Test
    public void doubleAvg_3Nodes_exceptionOnAccumulate() {
        IMap<String, Double> map = getMapWithNodeCount(3);
        populateMap(map);

        expected.expect(RuntimeException.class);
        expected.expectMessage("accumulate() exception");

        map.aggregate(new ExceptionThrowingAggregator<Map.Entry<String, Double>>(true, false, false));
    }

    @Test
    public void doubleAvg_3Nodes_exceptionOnCombine() {
        IMap<String, Double> map = getMapWithNodeCount(3);
        populateMap(map);

        expected.expect(RuntimeException.class);
        expected.expectMessage("combine() exception");

        map.aggregate(new ExceptionThrowingAggregator<Map.Entry<String, Double>>(false, true, false));
    }

    @Test
    public void doubleAvg_3Nodes_exceptionOnAggregate() {
        IMap<String, Double> map = getMapWithNodeCount(3);
        populateMap(map);

        expected.expect(RuntimeException.class);
        expected.expectMessage("aggregate() exception");

        map.aggregate(new ExceptionThrowingAggregator<Map.Entry<String, Double>>(false, false, true));
    }

    private IMap<String, Double> populateMap(IMap<String, Double> map) {
        map.put("key1", 1.0d);
        map.put("key2", 4.0d);
        map.put("key3", 7.0d);
        return map;
    }

    @Test
    public void doubleAvg_1Node_objectValue() {
        IMap<String, Person> map = getMapWithNodeCount(1);
        populateMapWithPersons(map);

        Double avg = map.aggregate(new DoubleAverageAggregator<Map.Entry<String, Person>>("age"));
        assertEquals(Double.valueOf(4.0d), avg);
    }

    @Test
    public void doubleAvg_3Nodes_objectValue() {
        IMap<String, Person> map = getMapWithNodeCount(3);
        populateMapWithPersons(map);

        Double avg = map.aggregate(new DoubleAverageAggregator<Map.Entry<String, Person>>("age"));
        assertEquals(Double.valueOf(4.0d), avg);
    }

    @Test
    public void doubleAvg_1Node_objectValue_withPredicate() {
        IMap<String, Person> map = getMapWithNodeCount(1);
        populateMapWithPersons(map);

        Double avg = map.aggregate(new DoubleAverageAggregator<Map.Entry<String, Person>>("age"), greaterThan("age", 2.0d));
        assertEquals(Double.valueOf(5.5d), avg);
    }

    @Test
    public void doubleAvg_1Node_objectValue_withEmptyResultPredicate() {
        IMap<String, Person> map = getMapWithNodeCount(1);
        populateMapWithPersons(map);

        Double avg = map.aggregate(new DoubleAverageAggregator<Map.Entry<String, Person>>("age"),
                greaterThan("age", 30.0d));
        assertNull(avg);
    }

    @Test
    public void doubleAvg_3Nodes_objectValue_withPredicate() {
        IMap<String, Person> map = getMapWithNodeCount(3);
        populateMapWithPersons(map);

        Double avg = map.aggregate(new DoubleAverageAggregator<Map.Entry<String, Person>>("age"), greaterThan("age", 2.0d));
        assertEquals(Double.valueOf(5.5d), avg);
    }

    private IMap<String, Person> populateMapWithPersons(IMap<String, Person> map) {
        map.put("key1", new Person(1.0d));
        map.put("key2", new Person(4.0d));
        map.put("key3", new Person(7.0d));
        return map;
    }

    private static class ExceptionThrowingAggregator<I> implements Aggregator<I, Double> {

        private boolean throwOnAccumulate;
        private boolean throwOnCombine;
        private boolean throwOnAggregate;

        ExceptionThrowingAggregator(boolean throwOnAccumulate, boolean throwOnCombine, boolean throwOnAggregate) {
            this.throwOnAccumulate = throwOnAccumulate;
            this.throwOnCombine = throwOnCombine;
            this.throwOnAggregate = throwOnAggregate;
        }

        @Override
        public void accumulate(I entry) {
            if (throwOnAccumulate) {
                throw new RuntimeException("accumulate() exception");
            }
        }

        @Override
        public void combine(Aggregator aggregator) {
            if (throwOnCombine) {
                throw new RuntimeException("combine() exception");
            }
        }

        @Override
        public Double aggregate() {
            if (throwOnAggregate) {
                throw new RuntimeException("aggregate() exception");
            }
            return 0.0d;
        }
    }

    @Test
    @Ignore("needs 10G of heap to run fast")
    public void doubleAvg_1node_10millionValues() {
        IMap<Long, Double> map = getMapWithNodeCount(1);
        System.err.println("Initialising");

        int elementCount = 10000000;
        double value = 0;
        Map<Long, Double> values = new HashMap<Long, Double>(elementCount);
        for (long i = 0L; i < elementCount; i++) {
            values.put(i, value++);
        }

        System.err.println("Putting");
        map.putAll(values);

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

        System.err.println("Executing");
        long start = System.currentTimeMillis();
        Double avg = map.aggregate(new DoubleAverageAggregator<Map.Entry<Long, Double>>());
        long stop = System.currentTimeMillis();
        System.err.println("Finished avg in " + (stop - start) + " millis avg=" + avg);
        System.err.flush();
    }

    public static class Person implements DataSerializable {
        public double age;

        public Person() {
        }

        public Person(double age) {
            this.age = age;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeDouble(age);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            age = in.readDouble();
        }
    }

    public <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count < 1");
        }

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);

        Config config = new Config();
        config.setProperty("hazelcast.partition.count", "3");
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("aggr");
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        config.addMapConfig(mapConfig);

        doWithConfig(config);

        HazelcastInstance instance = factory.newInstances(config)[0];
        return instance.getMap("aggr");
    }

    public void doWithConfig(Config config) {
    }
}
