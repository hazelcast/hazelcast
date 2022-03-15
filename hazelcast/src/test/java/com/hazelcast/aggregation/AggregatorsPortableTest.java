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
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.spi.properties.ClusterProperty.AGGREGATION_ACCUMULATION_PARALLEL_EVALUATION;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AggregatorsPortableTest extends HazelcastTestSupport {

    @Test
    public void testConstructors() {
        assertUtilityConstructor(Aggregators.class);
    }

    @Test
    public void aggregate_emptyNullSkipped_nullInValues_nullFirst() {
        // GIVEN
        IMap<Integer, Car> map = getMapWithNodeCount(1, false, InMemoryFormat.OBJECT);
        Car cornerCaseCar = getCornerCaseCar(null, "1");
        map.put(0, cornerCaseCar);

        // WHEN
        List<String> accumulated = map.aggregate(new TestAggregator("wheels[any]"));

        // THEN
        assertThat(accumulated, containsInAnyOrder(null, "1"));
        assertThat(accumulated, hasSize(2));
    }

    @Test
    public void aggregate_emptyNullSkipped_nullInValues() {
        // GIVEN
        IMap<Integer, Car> map = getMapWithNodeCount(1, false, InMemoryFormat.OBJECT);
        Car cornerCaseCar = getCornerCaseCar("1", null, "2", null);
        map.put(0, cornerCaseCar);

        // WHEN
        List<String> accumulated = map.aggregate(new TestAggregator("wheels[any]"));

        // THEN
        assertThat(accumulated, containsInAnyOrder("1", null, "2", null));
        assertThat(accumulated, hasSize(4));
    }

    @Test
    public void aggregate_emptyNullSkipped_noNullInValues() {
        // GIVEN
        IMap<Integer, Car> map = getMapWithNodeCount(1, false, InMemoryFormat.OBJECT);
        Car cornerCaseCar = getCornerCaseCar("2", "1");
        map.put(0, cornerCaseCar);

        // WHEN
        List<String> accumulated = map.aggregate(new TestAggregator("wheels[any]"));

        // THEN
        assertThat(accumulated, containsInAnyOrder("1", "2"));
        assertThat(accumulated, hasSize(2));
    }

    @Test
    public void aggregate_nullFirstArray() {
        // GIVEN
        IMap<Integer, Car> map = getMapWithNodeCount(1, false, InMemoryFormat.OBJECT);
        Car cornerCaseCar = new Car();
        map.put(0, cornerCaseCar);

        // WHEN
        List<String> accumulated = map.aggregate(new TestAggregator("wheels[any]"));

        // THEN
        assertThat(accumulated, hasSize(0));
    }

    @Test
    public void aggregate_emptyFirstArray() {
        // GIVEN
        IMap<Integer, Car> map = getMapWithNodeCount(1, false, InMemoryFormat.OBJECT);
        Car cornerCaseCar = new Car();
        cornerCaseCar.wheels = new String[]{};
        map.put(0, cornerCaseCar);

        // WHEN
        List<String> accumulated = map.aggregate(new TestAggregator("wheels[any]"));


        // THEN
        assertThat(accumulated, hasSize(0));
    }

    private Car getCornerCaseCar(String... values) {
        Car car = new Car();
        car.wheels = values;
        return car;
    }

    private static class TestAggregator extends AbstractAggregator<Map.Entry<Integer, Car>, String, List<String>> {
        private List<String> accumulated = new ArrayList<String>();

        TestAggregator(String attribute) {
            super(attribute);
        }

        @Override
        protected void accumulateExtracted(Map.Entry<Integer, Car> entry, String value) {
            accumulated.add(value);
        }

        @Override
        public void combine(Aggregator aggregator) {
            accumulated.addAll(((TestAggregator) aggregator).accumulated);
        }

        @Override
        public List<String> aggregate() {
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

        config.getSerializationConfig().addPortableFactory(CarFactory.ID, new CarFactory());

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        HazelcastInstance instance = factory.newInstances(config)[0];
        return instance.getMap("aggr");
    }

    static class Car implements Portable {
        String[] wheels;

        @Override
        public int getFactoryId() {
            return CarFactory.ID;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeStringArray("wheels", wheels);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            wheels = reader.readStringArray("wheels");
        }
    }

    static class CarFactory implements PortableFactory {
        static final int ID = 1;

        @Override
        public Portable create(int classId) {
            if (classId == 1) {
                return new Car();
            } else {
                return null;
            }
        }
    }
}
