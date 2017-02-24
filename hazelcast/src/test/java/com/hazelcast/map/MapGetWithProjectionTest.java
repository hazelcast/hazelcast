package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.executor.Offloadable;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.projection.Projection;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapGetWithProjectionTest extends HazelcastTestSupport {

    @Parameterized.Parameter(0)
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT},
        });
    }

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test(expected = NullPointerException.class)
    public void null_projection() {
        getMapWithNodeCount(1).get("key1", null);
    }

    @Test
    public void projection_1Node_primitiveValue() {
        IMap<String, Person> map = getMapWithNodeCount(1);
        populateMapWithPersons(map);

        assertEquals(map.get("key1", proj(null)), 2.0d, 0.000001);
        assertEquals(map.get("key2", proj(null)), 5.0d, 0.000001);
        assertEquals(map.get("key3", proj(null)), 8.0d, 0.000001);
    }

    @Test
    public void projection_3Node_primitiveValue() {
        IMap<String, Person> map = getMapWithNodeCount(3);
        populateMapWithPersons(map);

        assertEquals(map.get("key1", proj(null)), 2.0d, 0.000001);
        assertEquals(map.get("key2", proj(null)), 5.0d, 0.000001);
        assertEquals(map.get("key3", proj(null)), 8.0d, 0.000001);
    }

    @Test
    public void projection_1Node_primitiveValue_withOffloading() {
        IMap<String, Person> map = getMapWithNodeCount(1);
        populateMapWithPersons(map);

        assertEquals(map.get("key1", proj(ASYNC_EXECUTOR)), 2.0d, 0.000001);
        assertEquals(map.get("key2", proj(ASYNC_EXECUTOR)), 5.0d, 0.000001);
        assertEquals(map.get("key3", proj(ASYNC_EXECUTOR)), 8.0d, 0.000001);
    }

    @Test
    public void projection_3Node_primitiveValue_withOffloading() {
        IMap<String, Person> map = getMapWithNodeCount(3);
        populateMapWithPersons(map);

        assertEquals(map.get("key1", proj(ASYNC_EXECUTOR)), 2.0d, 0.000001);
        assertEquals(map.get("key2", proj(ASYNC_EXECUTOR)), 5.0d, 0.000001);
        assertEquals(map.get("key3", proj(ASYNC_EXECUTOR)), 8.0d, 0.000001);
    }

    public <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count < 1");
        }

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);

        Config config = getConfig();
        config.setProperty("hazelcast.partition.count", "3");

        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("aggr");
        mapConfig.setInMemoryFormat(inMemoryFormat);
        config.addMapConfig(mapConfig);

        doWithConfig(config);

        HazelcastInstance instance = factory.newInstances(config)[0];
        return instance.getMap("aggr");
    }

    public void doWithConfig(Config config) {

    }

    private void populateMapWithPersons(IMap<String, Person> map) {
        map.put("key1", new Person(1.0d));
        map.put("key2", new Person(4.0d));
        map.put("key3", new Person(7.0d));
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

    public static class AgeProjection extends Projection<Person, Double> implements Offloadable {

        public final String executorName;

        public AgeProjection(String executorName) {
            this.executorName = executorName;
        }

        @Override
        public Double transform(Person input) {
            return input.age + 1.0d;
        }

        @Override
        public String getExecutorName() {
            return executorName;
        }
    }

    public AgeProjection proj(String execName) {
        return new AgeProjection(execName);
    }

}
