package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class InMemoryFormatTest extends HazelcastTestSupport {

    @Test
    public void equals() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);

        Config config = new Config();
        config.addMapConfig(new MapConfig("objectMap").setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT));
        config.addMapConfig(new MapConfig("cachedMap").setInMemoryFormat(MapConfig.InMemoryFormat.CACHED));
        config.addMapConfig(new MapConfig("binaryMap").setInMemoryFormat(MapConfig.InMemoryFormat.BINARY));

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        Pair v1 = new Pair("a", "1");
        Pair v2 = new Pair("a", "2");

        IMap<String, Pair> objectMap = hz.getMap("objectMap");
        IMap<String, Pair> cachedMap = hz.getMap("cachedMap");
        IMap<String, Pair> binaryMap = hz.getMap("binaryMap");

        objectMap.put("1", v1);
        cachedMap.put("1", v1);
        binaryMap.put("1", v1);

        assertTrue(objectMap.containsValue(v1));
        assertTrue(objectMap.containsValue(v2));

        assertTrue(cachedMap.containsValue(v1));
        assertTrue(cachedMap.containsValue(v2));

        assertTrue(binaryMap.containsValue(v1));
        assertFalse(binaryMap.containsValue(v2));
    }

    public static final class Pair implements Serializable {
        private final String significant;
        private final String insignificant;

        public Pair(String significant, String insignificant) {
            this.significant = significant;
            this.insignificant = insignificant;
        }

        @Override
        public boolean equals(Object thatObj) {
            if (this == thatObj) {
                return true;
            }
            if (thatObj == null || getClass() != thatObj.getClass()) {
                return false;
            }
            Pair that = (Pair) thatObj;
            return this.significant.equals(that.significant);
        }

        @Override
        public int hashCode() {
            return significant.hashCode();
        }
    }

}
