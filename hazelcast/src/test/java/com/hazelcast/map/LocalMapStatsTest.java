package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class LocalMapStatsTest extends HazelcastTestSupport {

    private final String name = "fooMap";

    @Test
    public void testLastAccessTime() throws InterruptedException {
        final TimeUnit timeUnit = TimeUnit.NANOSECONDS;
        final long startTime = timeUnit.toMillis(System.nanoTime());

        HazelcastInstance h1 = createHazelcastInstance();
        IMap<String, String> map1 = h1.getMap(name);

        String key = "key";
        map1.put(key, "value");

        long lastUpdateTime = map1.getLocalMapStats().getLastUpdateTime();
        assertTrue(lastUpdateTime >= startTime);

        Thread.sleep(5);
        map1.put(key, "value2");
        long lastUpdateTime2 = map1.getLocalMapStats().getLastUpdateTime();
        assertTrue(lastUpdateTime2 > lastUpdateTime);
    }

}
