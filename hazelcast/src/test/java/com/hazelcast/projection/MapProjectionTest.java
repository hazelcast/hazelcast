package com.hazelcast.projection;


import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
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
import java.util.Collection;
import java.util.Map;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapProjectionTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

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
    public void null_projection_and_predicate() {
        getMapWithNodeCount(1).project((Projection) null, (Predicate) null);
    }

    @Test
    public void projection_1Node_primitiveValue() {
        IMap<String, Double> map = populateMap(getMapWithNodeCount(1));

        Collection<Double> result = map.project(new PrimitiveValueIncrementingProjection());

        assertThat(result, containsInAnyOrder(2.0d, 5.0d, 8.0d));
    }

    @Test
    public void projection_3Nodes_primitiveValue() {
        IMap<String, Double> map = populateMap(getMapWithNodeCount(3));

        Collection<Double> result = map.project(new PrimitiveValueIncrementingProjection());

        assertThat(result, containsInAnyOrder(2.0d, 5.0d, 8.0d));
    }

    @Test
    public void projection_3Nodes_primitiveValue_exceptionThrowingProjection() {
        IMap<String, Double> map = populateMap(getMapWithNodeCount(3));

        expected.expect(RuntimeException.class);
        expected.expectMessage("transform() exception");

        map.project(new ExceptionThrowingProjection());
    }

    @Test
    public void projection_3Nodes_nullReturningProjection() {
        IMap<String, Double> map = populateMap(getMapWithNodeCount(3));

        Collection<Double> result = map.project(new NullReturningProjection());

        assertThat(result, containsInAnyOrder((Double) null, null, null));
    }

    private IMap<String, Double> populateMap(IMap map) {
        map.put("key1", 1.0d);
        map.put("key2", 4.0d);
        map.put("key3", 7.0d);
        return map;
    }

    @Test
    public void projection_1Node_objectValue() {
        IMap<String, Person> map = populateMapWithPersons(getMapWithNodeCount(1));

        Collection<Double> result = map.project(new ObjectValueIncrementingProjection());

        assertThat(result, containsInAnyOrder(2.0d, 5.0d, 8.0d));
    }

    @Test
    public void projection_3Nodes_objectValue() {
        IMap<String, Person> map = populateMapWithPersons(getMapWithNodeCount(3));

        Collection<Double> result = map.project(new ObjectValueIncrementingProjection());

        assertThat(result, containsInAnyOrder(2.0d, 5.0d, 8.0d));
    }

    @Test
    public void projection_1Node_objectValue_withPredicate() {
        IMap<String, Person> map = populateMapWithPersons(getMapWithNodeCount(1));

        Collection<Double> result = map.project(new ObjectValueIncrementingProjection(), Predicates.greaterThan("age", 1.0d));

        assertThat(result, containsInAnyOrder(5.0d, 8.0d));
    }

    @Test
    public void projection_3Nodes_objectValue_withPredicate() {
        IMap<String, Person> map = populateMapWithPersons(getMapWithNodeCount(3));

        Collection<Double> result = map.project(new ObjectValueIncrementingProjection(), Predicates.greaterThan("age", 1.0d));

        assertThat(result, containsInAnyOrder(5.0d, 8.0d));
    }

    private IMap<String, Person> populateMapWithPersons(IMap map) {
        map.put("key1", new Person(1.0d));
        map.put("key2", new Person(4.0d));
        map.put("key3", new Person(7.0d));
        return map;
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

    public static class ExceptionThrowingProjection extends Projection {
        @Override
        public Object transform(Object input) {
            throw new RuntimeException("transform() exception");
        }
    }

    public static class NullReturningProjection extends Projection {
        @Override
        public Object transform(Object input) {
            return null;
        }
    }


    public static class PrimitiveValueIncrementingProjection extends Projection<Map.Entry<String, Double>, Double> {
        @Override
        public Double transform(Map.Entry<String, Double> input) {
            return input.getValue() + 1.0d;
        }
    }

    public static class ObjectValueIncrementingProjection extends Projection<Map.Entry<String, Person>, Double> {
        @Override
        public Double transform(Map.Entry<String, Person> input) {
            return input.getValue().age + 1.0d;
        }
    }

}
