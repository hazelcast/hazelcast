package com.hazelcast.query.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class IndexSplitBrainTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
    
    @Test
    public void testIndexDoesNotReturnStaleResultsAfterSplit() {
        String mapName = randomMapName();
        Config config = newConfig(mapName);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);

        warmUpPartitions(h1, h2);
        String key = generateKeyOwnedBy(h1);

        final ValueObject value = new ValueObject(key);

        final IMap<String, ValueObject> map1 = h1.getMap(mapName);
        final IMap<String, ValueObject> map2 = h2.getMap(mapName);

        map1.put(key, value);
        assertNotNull("Entry should exist in map2 before split", map2.get(key));

        // create split 
        closeConnectionBetween(h1, h2);
        assertClusterSizeEventually(1, h1);
        assertClusterSizeEventually(1, h2);

        map1.remove(key);
        assertNotNull("Entry should exist in map2 during split", map2.get(key));

        // merge back
        getNode(h2).getClusterService().merge(getAddress(h1));
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);

        assertNotNull("Entry should exist in map1 after merge", map1.get(key));
        map1.remove(key);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                Predicate predicate = Predicates.equal("id", value.getId());
                Collection<ValueObject> values = map1.values(predicate);
                assertThat(values, empty());

                values = map2.values(predicate);
                assertThat(values, empty());
            }
        }, 5);
    }

    private Config newConfig(String mapName) {
        Config config = new Config();

        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "600");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "600");

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addMapIndexConfig(new MapIndexConfig("id", false));

        return config;
    }

    private static class ValueObject implements DataSerializable {
        private String id;

        public ValueObject() {
        }

        ValueObject(String id) {
            this.id = id;
        }

        public String getId() {
            return this.id;
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(this.id);
        }

        public void readData(ObjectDataInput in) throws IOException {
            this.id = in.readUTF();
        }
    }
}
