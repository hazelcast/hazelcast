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
public class MultiAttributeProjectionTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test(expected = IllegalArgumentException.class)
    public void multiAttribute_attributeNull() {
        Projections.multiAttribute((String) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void multiAttribute_attributeEmpty() {
        Projections.multiAttribute("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void multiAttribute_attributeNullWithOther() {
        Projections.multiAttribute("age", null, "height");
    }

    @Test(expected = IllegalArgumentException.class)
    public void multiAttribute_attributeEmptyWithOther() {
        Projections.multiAttribute("age", "", "height");
    }

    @Test
    public void multiAttribute() {
        IMap<String, Person> map = getMapWithNodeCount();
        populateMapWithPersons(map);

        Collection<Object[]> result = map.project(Projections.multiAttribute("age", "height"));
        assertThat(result, containsInAnyOrder(new Object[]{1.0d, 190}, new Object[]{4.0d, 123}));
    }

    @Test
    public void multiAttribute_emptyMap() {
        IMap<String, Person> map = getMapWithNodeCount();

        Collection<Object[]> result = map.project(Projections.multiAttribute("age", "height"));

        assertEquals(0, result.size());
    }

    @Test
    public void multiAttribute_key() {
        IMap<String, Person> map = getMapWithNodeCount();
        populateMapWithPersons(map);

        Collection<Object[]> result = map.project(Projections.multiAttribute("__key"));

        assertThat(result, containsInAnyOrder(new Object[]{"key1"}, new Object[]{"key2"}));
    }

    @Test
    public void multiAttribute_this() {
        IMap<String, Integer> map = getMapWithNodeCount();
        map.put("key1", 1);
        map.put("key2", 2);

        Collection<Object[]> result = map.project(Projections.multiAttribute("this"));

        assertThat(result, containsInAnyOrder(new Object[]{1}, new Object[]{2}));
    }

    @Test
    public void multiAttribute_null() {
        IMap<String, Person> map = getMapWithNodeCount();
        map.put("key1", new Person(1.0d, null));
        map.put("007", new Person(null, 144));

        Collection<Object[]> result = map.project(Projections.multiAttribute("age", "height"));

        assertThat(result, containsInAnyOrder(new Object[]{1.0d, null}, new Object[]{null, 144}));
    }

    @Test
    public void multiAttribute_optional() {
        IMap<String, Person> map = getMapWithNodeCount();
        map.put("key1", new Person(1.0d, null));
        map.put("007", new Person(null, 144));

        Collection<Object[]> result = map.project(Projections.multiAttribute("optionalAge", "optionalHeight"));

        assertThat(result, containsInAnyOrder(new Object[]{1.0d, null}, new Object[]{null, 144}));
    }

    @Test
    public void multiAttribute_nonExistingProperty() {
        IMap<String, Person> map = getMapWithNodeCount();
        populateMapWithPersons(map);

        Projection<Map.Entry<String, Person>, Object[]> projection = Projections.multiAttribute("age", "height123");

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
        map.put("key1", new Person(1.0d, 190));
        map.put("key2", new Person(4.0d, 123));
    }

    public static class Person implements DataSerializable {

        public Double age;
        public Integer height;

        public Person() {
        }

        public Person(Double age, Integer height) {
            this.age = age;
            this.height = height;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(age);
            out.writeObject(height);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            age = in.readObject();
            height = in.readObject();
        }

        @SuppressWarnings("unused")
        public Optional<Double> optionalAge() {
            return Optional.ofNullable(age);
        }

        @SuppressWarnings("unused")
        public Optional<Integer> optionalHeight() {
            return Optional.ofNullable(height);
        }

    }

}
