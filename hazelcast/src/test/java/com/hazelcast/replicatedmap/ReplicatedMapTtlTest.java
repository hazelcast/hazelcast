package com.hazelcast.replicatedmap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.SlowTest;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(value = {SlowTest.class, ParallelTest.class})
public class ReplicatedMapTtlTest extends ReplicatedMapBaseTest {

    @Test
    public void testPutWithTTL() throws Exception {
        int nodeCount = 5;
        int keyCount = 20000;
        int operationCount = 20000;
        int threadCount = 15;
        int ttl = 500;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = factory.newInstances(null, nodeCount);
        String mapName = randomMapName();
        List<ReplicatedMap> maps = createMapOnEachInstance(instances, mapName);
        ArrayList<Integer> keys = generateRandomIntegerList(keyCount);
        Thread[] threads = createThreads(threadCount, maps, keys, ttl, timeUnit, operationCount);
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        for (ReplicatedMap map : maps) {
            assertSizeEventually(0, map, 60);
        }

    }

    private Thread[] createThreads(int count, List<ReplicatedMap> maps, ArrayList<Integer> keys,
                                   long ttl, TimeUnit timeunit, int operations) {
        Thread[] threads = new Thread[count];
        int size = maps.size();
        for (int i = 0; i < count; i++) {
            threads[i] = createPutOperationThread(maps.get(i % size), keys, ttl, timeunit, operations);
        }
        return threads;
    }

    private Thread createPutOperationThread(final ReplicatedMap<String, Object> map, final ArrayList<Integer> keys,
                                            final long ttl, final TimeUnit timeunit, final int operations) {
        return new Thread(new Runnable() {
            @Override
            public void run() {
                Random random = new Random();
                int size = keys.size();
                for (int i = 0; i < operations; i++) {
                    int index = i % size;
                    String key = "foo-" + keys.get(index);
                    map.put(key, random.nextLong(), 1 + random.nextInt((int) ttl), timeunit);
                }
            }
        });
    }

}
