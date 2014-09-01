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

import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MultiMapBaseAggregationTest
        extends AbstractAggregationTest {

    @Test
    public void testCountAggregation()
            throws Exception {

        String mapName = randomMapName();
        MultiMap<String, Integer> map = HAZELCAST_INSTANCE.getMultiMap(mapName);

        Integer[] values = buildPlainValues(new ValueProvider<Integer>() {
            @Override
            public Integer provideRandom(Random random) {
                return random(1000, 2000);
            }
        }, Integer.class);

        for (int i = 0; i < values.length; i++) {
            map.put("key-" + i, values[i]);
        }

        Supplier<String, Integer, Object> supplier = Supplier.all();
        Aggregation<String, Object, Long> aggregation = Aggregations.count();
        long count = map.aggregate(supplier, aggregation);
        assertEquals(values.length, count);
    }

    @Test
    public void testKeyPredicateAggregation()
            throws Exception {

        String mapName = randomMapName();
        MultiMap<Integer, Integer> map = HAZELCAST_INSTANCE.getMultiMap(mapName);

        Integer[] values = buildPlainValues(new ValueProvider<Integer>() {
            @Override
            public Integer provideRandom(Random random) {
                return random(1000, 2000);
            }
        }, Integer.class);

        for (int i = 0; i < values.length; i++) {
            map.put(i, values[i]);
        }

        KeyPredicate<Integer> keyPredicate = new SelectorKeyPredicate(values.length / 2);
        Supplier<Integer, Integer, Object> supplier = Supplier.fromKeyPredicate(keyPredicate);
        Aggregation<Integer, Object, Long> aggregation = Aggregations.count();
        long count = map.aggregate(supplier, aggregation);
        assertEquals(values.length / 2, count);
    }

    @Test
    public void testKeyPredicateAndExtractionAggregation()
            throws Exception {

        String mapName = randomMapName();
        MultiMap<Integer, Integer> map = HAZELCAST_INSTANCE.getMultiMap(mapName);

        Integer[] values = buildPlainValues(new ValueProvider<Integer>() {
            @Override
            public Integer provideRandom(Random random) {
                return random(1000, 2000);
            }
        }, Integer.class);

        for (int i = 0; i < values.length; i++) {
            map.put(i, values[i]);
        }

        KeyPredicate<Integer> keyPredicate = new SelectorKeyPredicate(values.length / 2);
        Supplier<Integer, Integer, Object> extractorSupplier = Supplier.all(new Extractor());
        Supplier<Integer, Integer, Object> supplier = Supplier.fromKeyPredicate(keyPredicate, extractorSupplier);
        Aggregation<Integer, Object, Long> aggregation = Aggregations.count();
        long count = map.aggregate(supplier, aggregation);
        assertEquals(values.length / 2, count);
    }

    @Test
    public void testPredicateAggregation()
            throws Exception {

        String mapName = randomMapName();
        MultiMap<Integer, Integer> map = HAZELCAST_INSTANCE.getMultiMap(mapName);

        Integer[] values = buildPlainValues(new ValueProvider<Integer>() {
            @Override
            public Integer provideRandom(Random random) {
                return random(1000, 2000);
            }
        }, Integer.class);

        for (int i = 0; i < values.length; i++) {
            map.put(i, values[i]);
        }

        Predicate<Integer, Integer> predicate = new SelectorPredicate(values.length / 2);
        Supplier<Integer, Integer, Object> supplier = Supplier.fromPredicate(predicate);
        Aggregation<Integer, Object, Long> aggregation = Aggregations.count();
        long count = map.aggregate(supplier, aggregation);
        assertEquals(values.length / 2, count);
    }

    @Test
    public void testPredicateAndExtractionAggregation()
            throws Exception {

        String mapName = randomMapName();
        MultiMap<Integer, Integer> map = HAZELCAST_INSTANCE.getMultiMap(mapName);

        Integer[] values = buildPlainValues(new ValueProvider<Integer>() {
            @Override
            public Integer provideRandom(Random random) {
                return random(1000, 2000);
            }
        }, Integer.class);

        for (int i = 0; i < values.length; i++) {
            map.put(i, values[i]);
        }

        Predicate<Integer, Integer> predicate = new SelectorPredicate(values.length / 2);
        Supplier<Integer, Integer, Object> extractorSupplier = Supplier.all(new Extractor());
        Supplier<Integer, Integer, Object> supplier = Supplier.fromPredicate(predicate, extractorSupplier);
        Aggregation<Integer, Object, Long> aggregation = Aggregations.count();
        long count = map.aggregate(supplier, aggregation);
        assertEquals(values.length / 2, count);
    }

    @Test
    public void testDistinctValuesAggregation()
            throws Exception {

        final String[] probes = {"Dog", "Food", "Champion", "Hazelcast", "Security", "Integer", "Random", "System"};
        Set<String> expectation = new HashSet<String>(Arrays.asList(probes));

        String mapName = randomMapName();
        MultiMap<String, String> map = HAZELCAST_INSTANCE.getMultiMap(mapName);

        String[] values = buildPlainValues(new ValueProvider<String>() {
            @Override
            public String provideRandom(Random random) {
                int index = random.nextInt(probes.length);
                return probes[index];
            }
        }, String.class);

        for (int i = 0; i < values.length; i++) {
            map.put("key-" + i, values[i]);
        }

        Supplier<String, String, String> supplier = Supplier.all();
        Aggregation<String, String, Set<String>> aggregation = Aggregations.distinctValues();
        Set<String> distinctValues = map.aggregate(supplier, aggregation);
        assertEquals(expectation, distinctValues);

    }

    public static class SelectorKeyPredicate implements KeyPredicate<Integer> {

        private int maxKey;

        public SelectorKeyPredicate() {
        }

        public SelectorKeyPredicate(int maxKey) {
            this.maxKey = maxKey;
        }

        @Override
        public boolean evaluate(Integer key) {
            return key != null && key < maxKey;
        }
    }

    public static class SelectorPredicate implements Predicate<Integer, Integer> {

        private int maxKey;

        public SelectorPredicate() {
        }

        public SelectorPredicate(int maxKey) {
            this.maxKey = maxKey;
        }

        @Override
        public boolean apply(Map.Entry<Integer, Integer> mapEntry) {
            Integer key = mapEntry.getKey();
            return key != null && key < maxKey;
        }

        @Override
        public boolean isSubSet(Predicate predicate) {
            return false;
        }
    }

    public static class Extractor implements PropertyExtractor<Integer, Object> {

        @Override
        public Object extract(Integer value) {
            return value;
        }
    }
}
