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

package com.hazelcast.projection;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.google.common.primitives.Ints.asList;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapProjectionTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    protected HazelcastInstance instance0;

    @Test(expected = NullPointerException.class)
    public void null_projection() {
        getMapWithNodeCount(1).project(null);
    }

    @Test(expected = NullPointerException.class)
    public void null_predicate() {
        IMap<String, Double> map = getMapWithNodeCount(1);
        map.project(new PrimitiveValueIncrementingProjection(), null);
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings({"RedundantCast", "unchecked"})
    public void null_projection_and_predicate() {
        getMapWithNodeCount(1).project((Projection) null, (Predicate) null);
    }

    @Test(expected = IllegalArgumentException.class)
    @SuppressWarnings("RedundantCast")
    public void pagingPredicate_fails() {
        getMapWithNodeCount(1).project(new NullReturningProjection(), Predicates.pagingPredicate(1));
    }

    @Test
    public void projection_1Node_primitiveValue() {
        IMap<String, Double> map = getMapWithNodeCount(3);
        populateMap(map);

        Collection<Double> result = map.project(new PrimitiveValueIncrementingProjection());

        assertThat(result, containsInAnyOrder(2.0d, 5.0d, 8.0d));
    }

    @Test
    public void projection_3Nodes_primitiveValue() {
        IMap<String, Double> map = getMapWithNodeCount(3);
        populateMap(map);

        Collection<Double> result = map.project(new PrimitiveValueIncrementingProjection());

        assertThat(result, containsInAnyOrder(2.0d, 5.0d, 8.0d));
    }

    @Test
    public void projection_3Nodes_primitiveValue_exceptionThrowingProjection() {
        IMap<String, Double> map = getMapWithNodeCount(3);
        populateMap(map);

        expected.expect(RuntimeException.class);
        expected.expectMessage("transform() exception");

        map.project(new ExceptionThrowingProjection());
    }

    @Test
    public void projection_3Nodes_nullReturningProjection() {
        IMap<String, Double> map = getMapWithNodeCount(3);
        populateMap(map);

        Collection<Double> result = map.project(new NullReturningProjection());

        assertThat(result, containsInAnyOrder((Double) null, null, null));
    }

    @Test
    public void projection_1Node_objectValue() {
        IMap<String, Person> map = getMapWithNodeCount(1);
        populateMapWithPersons(map);

        Collection<Double> result = map.project(new ObjectValueIncrementingProjection());

        assertThat(result, containsInAnyOrder(2.0d, 5.0d, 8.0d));
    }

    @Test
    public void projection_3Nodes_objectValue() {
        IMap<String, Person> map = getMapWithNodeCount(3);
        populateMapWithPersons(map);

        Collection<Double> result = map.project(new ObjectValueIncrementingProjection());

        assertThat(result, containsInAnyOrder(2.0d, 5.0d, 8.0d));
    }

    @Test
    public void projection_3Nodes_multiAttribute() {
        IMap<String, Person> map = getMapWithNodeCount(3);
        populateMapWithPersons(map);

        Collection<Object[]> result = map.project(Projections.multiAttribute("age", "state"));

        assertThat(result, containsInAnyOrder(new Object[]{1.0d, "NY"}, new Object[]{4.0d, "DC"}, new Object[]{7.0d, "OH"}));
    }

    @Test
    public void projection_1Node_objectValue_withPredicate() {
        IMap<String, Person> map = getMapWithNodeCount(1);
        populateMapWithPersons(map);

        Collection<Double> result = map.project(new ObjectValueIncrementingProjection(), Predicates.greaterThan("age", 1.0d));

        assertThat(result, containsInAnyOrder(5.0d, 8.0d));
    }

    @Test
    public void projection_3Nodes_objectValue_withPredicate() {
        IMap<String, Person> map = getMapWithNodeCount(3);
        populateMapWithPersons(map);

        Collection<Double> result = map.project(new ObjectValueIncrementingProjection(), Predicates.greaterThan("age", 1.0d));

        assertThat(result, containsInAnyOrder(5.0d, 8.0d));
    }

    @Test
    public void projection_1Node_objectValue_withPartitionSet() {
        IMap<String, Person> map = getMapWithNodeCount(1);
        populateMapWithPersons(map);
        PartitionIdSet partitionSubset = new PartitionIdSet(3, asList(0, 1));

        Collection<Double> result = ((MapProxyImpl<String, Person>) map)
                .project(new ObjectValueIncrementingProjection(), Predicates.alwaysTrue(), partitionSubset);

        assertThat(result, containsInAnyOrder(2.0d, 5.0d));
    }

    @Test
    public void projection_3Nodes_objectValue_withPartitionSet() {
        IMap<String, Person> map = getMapWithNodeCount(3);
        populateMapWithPersons(map);
        PartitionIdSet partitionSubset = new PartitionIdSet(3, asList(0, 1));

        Collection<Double> result = ((MapProxyImpl<String, Person>) map)
                .project(new ObjectValueIncrementingProjection(), Predicates.alwaysTrue(), partitionSubset);

        assertThat(result, containsInAnyOrder(2.0d, 5.0d));
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

        instance0 = factory.newInstances(config)[0];
        return instance0.getMap("aggr");
    }

    // used by hz-enterprise
    public void doWithConfig(Config config) {
    }

    private void populateMap(IMap<String, Double> map) {
        map.put("key1", 1.0d);
        map.put("key2", 4.0d);
        map.put("key3", 7.0d);
    }

    private void populateMapWithPersons(IMap<String, Person> map) {
        map.put(generateKeyForPartition(instance0, 0), new Person(1.0d, "NY"));
        map.put(generateKeyForPartition(instance0, 1), new Person(4.0d, "DC"));
        map.put(generateKeyForPartition(instance0, 2), new Person(7.0d, "OH"));
    }

    public static class Person implements DataSerializable {

        public double age;
        public String state;

        public Person() {
        }

        public Person(double age) {
            this.age = age;
        }

        public Person(double age, String state) {
            this.age = age;
            this.state = state;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeDouble(age);
            out.writeString(state);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            age = in.readDouble();
            state = in.readString();
        }
    }

    public static class ExceptionThrowingProjection implements Projection {
        @Override
        public Object transform(Object input) {
            throw new RuntimeException("transform() exception");
        }
    }

    public static class NullReturningProjection implements Projection {
        @Override
        public Object transform(Object input) {
            return null;
        }
    }


    public static class PrimitiveValueIncrementingProjection implements Projection<Map.Entry<String, Double>, Double> {
        @Override
        public Double transform(Map.Entry<String, Double> input) {
            return input.getValue() + 1.0d;
        }
    }

    public static class ObjectValueIncrementingProjection implements Projection<Map.Entry<String, Person>, Double> {
        @Override
        public Double transform(Map.Entry<String, Person> input) {
            return input.getValue().age + 1.0d;
        }
    }
}
