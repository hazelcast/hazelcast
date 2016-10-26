package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartitionPredicateTest extends HazelcastTestSupport {

    private static final int PARTITIONS = 10;
    private static final int ITEMS_PER_PARTITION = 20;

    private HazelcastInstance local;
    private IMap<String, Integer> map;

    private String partitionKey;
    private int partitionId;

    private Predicate<String, Integer> predicate;

    @Before
    public void setup() {
        Config config = getConfig()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "" + PARTITIONS);

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        local = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance remote = nodeFactory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        map = local.getMap(randomString());
        for (int p = 0; p < PARTITIONS; p++) {
            for (int k = 0; k < ITEMS_PER_PARTITION; k++) {
                map.put(generateKeyForPartition(local, p), p);
            }
        }

        partitionKey = randomString();
        partitionId = local.getPartitionService().getPartition(partitionKey).getPartitionId();

        predicate = new PartitionPredicate<String, Integer>(partitionKey, TruePredicate.INSTANCE);
    }

    @Test
    public void values() {
        Collection<Integer> values = map.values(predicate);

        assertEquals(ITEMS_PER_PARTITION, values.size());
        for (Integer value : values) {
            assertEquals(partitionId, value.intValue());
        }
    }

    @Test
    public void keySet() {
        Collection<String> keys = map.keySet(predicate);

        assertEquals(ITEMS_PER_PARTITION, keys.size());
        for (String key : keys) {
            assertEquals(partitionId, local.getPartitionService().getPartition(key).getPartitionId());
        }
    }

    @Test
    public void entries() {
        Collection<Map.Entry<String,Integer>> entries = map.entrySet(predicate);

        assertEquals(ITEMS_PER_PARTITION, entries.size());
        for (Map.Entry<String,Integer> entry : entries) {
            assertEquals(partitionId, local.getPartitionService().getPartition(entry.getKey()).getPartitionId());
            assertEquals(partitionId, entry.getValue().intValue());
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void apply() {
        assertTrue(predicate.apply(null));
    }

    @Test
    public void testToString() {
        assertTrue(predicate.toString().contains("PartitionPredicate"));
        assertTrue(predicate.toString().contains("partitionKey=" + partitionKey));
    }
}
