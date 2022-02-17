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

package com.hazelcast.projection.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.projection.Projection;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.QueryException;
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
import java.util.Optional;

import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SingleAttributeProjectionTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test(expected = IllegalArgumentException.class)
    public void singleAttribute_attributeNull() {
        Projections.<Map.Entry<String, Person>, Double>singleAttribute(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void singleAttribute_attributeEmpty() {
        Projections.<Map.Entry<String, Person>, Double>singleAttribute("");
    }

    @Test
    public void singleAttribute() {
        IMap<String, Person> map = getMapWithNodeCount();
        populateMapWithPersons(map);

        Collection<Double> result = map.project(Projections.singleAttribute("age"));

        assertThat(result, containsInAnyOrder(1.0d, 4.0d, 7.0d));
    }

    @Test
    public void singleAttribute_key() {
        IMap<String, Person> map = getMapWithNodeCount();
        populateMapWithPersons(map);

        Collection<String> result = map.project(Projections.singleAttribute("__key"));

        assertThat(result, containsInAnyOrder("key1", "key2", "key3"));
    }

    @Test
    public void singleAttribute_this() {
        IMap<String, Integer> map = getMapWithNodeCount();
        map.put("key1", 1);
        map.put("key2", 2);

        Collection<Integer> result = map.project(Projections.singleAttribute("this"));

        assertThat(result, containsInAnyOrder(1, 2));
    }

    @Test
    public void singleAttribute_emptyMap() {
        IMap<String, Person> map = getMapWithNodeCount();

        Collection<Double> result = map.project(Projections.singleAttribute("age"));

        assertEquals(0, result.size());
    }

    @Test
    public void singleAttribute_null() {
        IMap<String, Person> map = getMapWithNodeCount();
        map.put("key1", new Person(1.0d));
        map.put("007", new Person(null));

        Collection<Double> result = map.project(Projections.singleAttribute("age"));

        assertThat(result, containsInAnyOrder(null, 1.0d));
    }

    @Test
    public void singleAttribute_optional() {
        IMap<String, Person> map = getMapWithNodeCount();
        map.put("key1", new Person(1.0d));
        map.put("007", new Person(null));

        Collection<Double> result = map.project(Projections.singleAttribute("optionalAge"));

        assertThat(result, containsInAnyOrder(null, 1.0d));
    }

    @Test
    public void singleAttribute_nonExistingProperty() {
        IMap<String, Person> map = getMapWithNodeCount();
        populateMapWithPersons(map);

        Projection<Map.Entry<String, Person>, Double> projection = Projections.singleAttribute("age123");

        expected.expect(QueryException.class);
        map.project(projection);
    }

    private <K, V> IMap<K, V> getMapWithNodeCount() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);

        MapConfig mapConfig = new MapConfig()
                .setName("aggr")
                .setInMemoryFormat(InMemoryFormat.OBJECT);

        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "3")
                .addMapConfig(mapConfig);

        HazelcastInstance instance = factory.newInstances(config)[0];
        return instance.getMap("aggr");
    }

    private void populateMapWithPersons(IMap<String, Person> map) {
        map.put("key1", new Person(1.0d));
        map.put("key2", new Person(4.0d));
        map.put("key3", new Person(7.0d));
    }

    public static class Person implements DataSerializable {

        public Double age;

        public Person() {
        }

        public Person(Double age) {
            this.age = age;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(age);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            age = in.readObject();
        }

        @SuppressWarnings("unused")
        public Optional<Double> optionalAge() {
            return Optional.ofNullable(age);
        }

    }

}
