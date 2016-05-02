package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.eviction.MapEvictionPolicy;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_PARTITION;
import static com.hazelcast.map.impl.eviction.Evictor.SAMPLE_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static java.lang.String.format;
import static junit.framework.TestCase.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapEvictionPolicyTest extends HazelcastTestSupport {

    private final String mapName = "default";

    @Test
    public void testMapEvictionPolicy() throws Exception {
        int sampleCount = SAMPLE_COUNT;

        Config config = getConfig();
        config.setProperty(PARTITION_COUNT.getName(), "1");
        config.getMapConfig(mapName)
                .setMapEvictionPolicy(new OddEvictor())
                .getMaxSizeConfig()
                .setMaxSizePolicy(PER_PARTITION).setSize(sampleCount);

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, Integer> map = instance.getMap(mapName);

        final CountDownLatch eventLatch = new CountDownLatch(1);
        final Queue<Integer> evictedKeys = new ConcurrentLinkedQueue<Integer>();
        map.addEntryListener(new EntryEvictedListener<Integer, Integer>() {
            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
                evictedKeys.add(event.getKey());
                eventLatch.countDown();
            }
        }, false);

        for (int i = 0; i < sampleCount + 1; i++) {
            map.put(i, i);
        }

        assertOpenEventually("No eviction occurred", eventLatch);

        for (Integer key : evictedKeys) {
            assertTrue(format("Evicted key should be an odd number, but found %d", key), key % 2 != 0);
        }
    }

    private static class OddEvictor extends MapEvictionPolicy {

        @Override
        public int compare(EntryView o1, EntryView o2) {
            Integer key = (Integer) o1.getKey();
            if (key % 2 != 0) {
                return -1;
            }

            return 1;
        }
    }
}