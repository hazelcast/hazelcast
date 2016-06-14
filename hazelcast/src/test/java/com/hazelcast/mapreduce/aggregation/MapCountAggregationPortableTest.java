package com.hazelcast.mapreduce.aggregation;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MapCountAggregationPortableTest extends HazelcastTestSupport {

    private HazelcastInstance hazelcastInstance;

    @Before
    public void before() {
        Config config = new Config();
        config.getSerializationConfig().addPortableFactory(1, new FooPortableFactory());
        this.hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    }

    @After
    public void after() {
        this.hazelcastInstance.shutdown();
    }

    @Test
    public void testCountAggregationWithPortable() {
        // GIVEN
        IMap<Integer, Foo> fooMap = hazelcastInstance.getMap("fooMap");
        Foo value = new Foo();
        value.text = "foo";
        fooMap.put(1, value);

        // WHEN
        Supplier<Integer, Foo, Integer> supplier = Supplier.fromPredicate(Predicates.equal("text", "foo"));
        Aggregation<Integer, Integer, Long> counter = Aggregations.count();
        long count = fooMap.aggregate(supplier, counter);

        // THEN
        assertEquals(1L, count);
    }

    private static class FooPortableFactory implements PortableFactory {
        public Portable create(int classId) {
            if (Foo.ID == classId) {
                return new Foo();
            } else {
                return null;
            }
        }
    }

    private static class Foo implements Portable {
        public static final int ID = 1;
        String text;

        public int getFactoryId() {
            return 1;
        }

        public int getClassId() {
            return ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeUTF("text", text);
        }

        public void readPortable(PortableReader reader) throws IOException {
            text = reader.readUTF("text");
        }
    }
}
