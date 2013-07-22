package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MapCacheTest {
    public static final String key = "Some key";
    public static AtomicInteger version = new AtomicInteger(1);
    public static int DURATION_SEC = 60;

    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() throws Exception {
        MapConfig mapConfig = new MapConfig("repo").setCacheValue(true);
        Config config = new Config().addMapConfig(mapConfig);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        BlockingQueue<Boolean> q = new ArrayBlockingQueue<Boolean>(1);

        Map<String, Integer> repo = hz.getMap("repo");
        repo.put(key, 1);

        final BreakerThread breakerThread = new BreakerThread(repo, q);
        breakerThread.start();
        WatchDog watchDog = new WatchDog(repo, q);
        watchDog.start();
        watchDog.join();

        try {
            assertFalse("the cache value was not updated", watchDog.failureDetected);
        } finally {
            breakerThread.interrupt();
        }

    }

    public class BreakerThread extends Thread {
        protected Map<String, Integer> hazelcast;
        protected BlockingQueue<Boolean> queue;

        public BreakerThread(Map<String, Integer> hazelcast, BlockingQueue<Boolean> queue) {
            this.hazelcast = hazelcast;
            this.queue = queue;
        }

        @Override
        public void run() {
            for (; ; ) {
                try {
                    queue.take();
                } catch (InterruptedException e) {
                    return;
                }
                Integer tmp = version.incrementAndGet();
                hazelcast.put(key, tmp);
            }
        }
    }

    public class WatchDog extends Thread {
        private int LOOP_MAX = 5;
        protected Map<String, Integer> hazelcast;
        protected BlockingQueue<Boolean> queue;
        private volatile boolean failureDetected = false;

        public WatchDog(Map<String, Integer> hazelcast, BlockingQueue<Boolean> queue) {
            this.hazelcast = hazelcast;
            this.queue = queue;
        }

        private boolean checkEquals() {
            int etalon = version.get();
            int hz = hazelcast.get(key);
            if (etalon == hz)
                System.out.println(etalon + " == " + hz);
            else
                System.out.println(etalon + " != " + hz + " " + (etalon - hz));
            return etalon == hz;
        }

        @Override
        public void run() {
            try {
                long startMs = System.currentTimeMillis();
                while (System.currentTimeMillis() < startMs + (DURATION_SEC * 1000)) {
                    boolean tmp = checkEquals();
                    int cnt = 0;
                    while (!tmp && (cnt++ < LOOP_MAX)) {
                        Thread.sleep(4);       // in most cases 1 ms is enough
                        tmp = checkEquals();
                    }
                    if (!tmp) {
                        Thread.sleep(5000);    // i think 5 sec is enough to update cache
                        if (!checkEquals()) {    // but, it's not
                            failureDetected = true;
                            return;
                        }
                    }
                    if (queue.peek() == null)
                        queue.put(true);

                }
            } catch (InterruptedException ignored) {
            }
        }
    }


}
