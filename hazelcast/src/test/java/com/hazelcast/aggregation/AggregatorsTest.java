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

import com.hazelcast.aggregation.impl.AbstractAggregator;
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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.spi.properties.ClusterProperty.AGGREGATION_ACCUMULATION_PARALLEL_EVALUATION;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AggregatorsTest extends HazelcastTestSupport {

    @Test
    public void testConstructors() {
        assertUtilityConstructor(Aggregators.class);
    }

    @Test
    public void aggregate_emptyNullSkipped_nullInValues() {
        // GIVEN
        IMap<Integer, Car> map = getMapWithNodeCount(1, false, InMemoryFormat.OBJECT);
        Car cornerCaseCar = getCornerCaseCar(null, 1L);
        map.put(0, cornerCaseCar);

        // WHEN
        List<Long> accumulatedArray = map.aggregate(new TestAggregator("wheelsA[any].tiresA[any]"));
        List<Long> accumulatedCollection = map.aggregate(new TestAggregator("wheelsC[any].tiresC[any]"));

        // THEN
        assertThat(accumulatedCollection, containsInAnyOrder(accumulatedArray.toArray()));
        assertThat(accumulatedArray, containsInAnyOrder(accumulatedCollection.toArray()));
        assertThat(accumulatedArray, containsInAnyOrder(1L, null));
        assertThat(accumulatedArray, hasSize(2));
    }

    @Test
    public void aggregate_emptyNullSkipped_noNullInValues() {
        // GIVEN
        IMap<Integer, Car> map = getMapWithNodeCount(1, false, InMemoryFormat.OBJECT);
        Car cornerCaseCar = getCornerCaseCar(2L, 1L);
        map.put(0, cornerCaseCar);

        // WHEN
        List<Long> accumulatedArray = map.aggregate(new TestAggregator("wheelsA[any].tiresA[any]"));
        List<Long> accumulatedCollection = map.aggregate(new TestAggregator("wheelsC[any].tiresC[any]"));

        // THEN
        assertThat(accumulatedCollection, containsInAnyOrder(accumulatedArray.toArray()));
        assertThat(accumulatedArray, containsInAnyOrder(accumulatedCollection.toArray()));
        assertThat(accumulatedArray, containsInAnyOrder(1L, 2L));
        assertThat(accumulatedArray, hasSize(2));
    }

    @Test
    public void aggregate_emptyNullSkipped_moreThanOneNullInValues() {
        // GIVEN
        IMap<Integer, Car> map = getMapWithNodeCount(1, false, InMemoryFormat.OBJECT);
        Car cornerCaseCar = getCornerCaseCar(2L, null, 1L, null);
        map.put(0, cornerCaseCar);

        // WHEN
        List<Long> accumulatedArray = map.aggregate(new TestAggregator("wheelsA[any].tiresA[any]"));
        List<Long> accumulatedCollection = map.aggregate(new TestAggregator("wheelsC[any].tiresC[any]"));

        // THEN
        assertThat(accumulatedCollection, containsInAnyOrder(accumulatedArray.toArray()));
        assertThat(accumulatedArray, containsInAnyOrder(accumulatedCollection.toArray()));
        assertThat(accumulatedArray, containsInAnyOrder(1L, 2L, null, null));
        assertThat(accumulatedArray, hasSize(4));
    }

    @Test
    public void aggregate_nullFirstArray() {
        // GIVEN
        IMap<Integer, Car> map = getMapWithNodeCount(1, false, InMemoryFormat.OBJECT);
        Car cornerCaseCar = new Car();
        map.put(0, cornerCaseCar);

        // WHEN
        List<Long> accumulatedArray = map.aggregate(new TestAggregator("wheelsA[any].tiresA[any]"));
        List<Long> accumulatedCollection = map.aggregate(new TestAggregator("wheelsC[any].tiresC[any]"));

        // THEN
        assertThat(accumulatedCollection, containsInAnyOrder(accumulatedArray.toArray()));
        assertThat(accumulatedArray, containsInAnyOrder(accumulatedCollection.toArray()));
        assertThat(accumulatedCollection, hasSize(0));
    }

    private Car getCornerCaseCar(Long... values) {
        Wheel nullWheel = new Wheel();

        Wheel emptyWheel = new Wheel();
        emptyWheel.tiresA = new Long[0];
        emptyWheel.tiresC = new ArrayList<Long>();

        Wheel wheel = new Wheel();
        wheel.tiresA = values;
        wheel.tiresC = new ArrayList<Long>();
        for (Long value : values) {
            wheel.tiresC.add(value);
        }

        Car car = new Car();
        car.wheelsA = new Wheel[]{wheel, emptyWheel, nullWheel};
        car.wheelsC = new ArrayList<Wheel>();
        car.wheelsC.add(emptyWheel);
        car.wheelsC.add(wheel);
        car.wheelsC.add(nullWheel);

        return car;
    }

    private static class TestAggregator extends AbstractAggregator<Map.Entry<Integer, Car>, Long, List<Long>> {
        private List<Long> accumulated = new ArrayList<Long>();

        TestAggregator(String attribute) {
            super(attribute);
        }

        @Override
        protected void accumulateExtracted(Map.Entry<Integer, Car> entry, Long value) {
            accumulated.add(value);
        }

        @Override
        public void combine(Aggregator aggregator) {
            accumulated.addAll(((TestAggregator) aggregator).accumulated);
        }

        @Override
        public List<Long> aggregate() {
            return accumulated;
        }
    }

    protected <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount, boolean parallelAccumulation, InMemoryFormat inMemoryFormat) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count < 1");
        }

        MapConfig mapConfig = new MapConfig()
                .setName("aggr")
                .setInMemoryFormat(inMemoryFormat);

        Config config = getConfig()
                .setProperty(PARTITION_COUNT.getName(), String.valueOf(nodeCount))
                .setProperty(AGGREGATION_ACCUMULATION_PARALLEL_EVALUATION.getName(), String.valueOf(parallelAccumulation))
                .addMapConfig(mapConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        HazelcastInstance instance = factory.newInstances(config)[0];
        return instance.getMap("aggr");
    }

    static class Car implements Serializable {
        Collection<Wheel> wheelsC;
        Wheel[] wheelsA;
    }

    static class Wheel implements Serializable {
        Collection<Long> tiresC;
        Long[] tiresA;
    }
}
