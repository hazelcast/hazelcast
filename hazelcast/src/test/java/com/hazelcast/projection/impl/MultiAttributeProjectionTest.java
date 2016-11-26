package com.hazelcast.projection.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.projection.Projection;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.QueryException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MultiAttributeProjectionTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void multiAttribute() {
        IMap<String, Person> map = populateMapWithPersons(getMapWithNodeCount(1));

        Collection<Object[]> result = map.project(Projections.<String, Person>multiAttribute("age", "height"));
        assertThat(result, containsInAnyOrder(new Object[]{1.0d, 190}, new Object[]{4.0d, 123}));
    }

    @Test
    public void multiAttribute_emptyMap() {
        IMap<String, Person> map = getMapWithNodeCount(1);

        Collection<Object[]> result = map.project(Projections.<String, Person>multiAttribute("age", "height"));

        assertEquals(0, result.size());
    }


    @Test
    public void multiAttribute_key() {
        IMap<String, Person> map = populateMapWithPersons(getMapWithNodeCount(1));

        Collection<Object[]> result = map.project(Projections.<String, Person>multiAttribute("__key"));

        assertThat(result, containsInAnyOrder(new Object[]{"key1"}, new Object[]{"key2"}));
    }

    @Test
    public void multiAttribute_this() {
        IMap<String, Integer> map = getMapWithNodeCount(1);
        map.put("key1", 1);
        map.put("key2", 2);

        Collection<Object[]> result = map.project(Projections.<String, Integer>multiAttribute("this"));

        assertThat(result, containsInAnyOrder(new Object[]{1}, new Object[]{2}));
    }

    @Test
    public void multiAttribute_null() {
        IMap<String, Person> map = getMapWithNodeCount(1);
        map.put("key1", new Person(1.0d, null));
        map.put("007", new Person(null, 144));

        Collection<Object[]> result = map.project(Projections.<String, Person>multiAttribute("age", "height"));

        assertThat(result, containsInAnyOrder(new Object[]{1.0d, null}, new Object[]{null, 144}));
    }

    @Test
    public void multiAttribute_nonExistingProperty() {
        IMap<String, Person> map = populateMapWithPersons(getMapWithNodeCount(1));

        Projection<Map.Entry<String, Person>, Object[]> projection = Projections.multiAttribute("age", "height123");

        expected.expect(QueryException.class);
        map.project(projection);
    }

    private IMap<String, Person> populateMapWithPersons(IMap map) {
        map.put("key1", new Person(1.0d, 190));
        map.put("key2", new Person(4.0d, 123));
        return map;
    }

    public <R> Collection<R> getAxis(int axis, Collection<Object[]> input) {
        Collection result = new ArrayList();
        for (Object[] o : input) {
            result.add(o[axis]);
        }
        return result;
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

        HazelcastInstance instance = factory.newInstances(config)[0];
        return instance.getMap("aggr");
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
    }

}
