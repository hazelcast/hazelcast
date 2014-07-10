package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.assertJoinable;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientMapTryLockConcurrentTests {

    static HazelcastInstance client;
    static HazelcastInstance server;

    @BeforeClass
    public static void init() {
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void concurrent_MapTryLockTest() throws InterruptedException {
        concurrent_MapTryLock(false);
    }

    @Test
    public void concurrent_MapTryLockTimeOutTest() throws InterruptedException {
        concurrent_MapTryLock(true);
    }

    private void concurrent_MapTryLock(boolean withTimeOut) throws InterruptedException {
        int maxThreads = 8;
        IMap<String, Integer> map = client.getMap(randomString());
        String upKey = "upKey";
        String downKey = "downKey";

        map.put(upKey, 0);
        map.put(downKey, 0);

        Thread threads[] = new Thread[maxThreads];
        for (int i = 0; i < threads.length; i++) {

            Thread thread;
            if (withTimeOut) {
                thread = new MapTryLockTimeOutThread(map, upKey, downKey);
            } else {
                thread = new MapTryLockThread(map, upKey, downKey);
            }
            thread.start();
            threads[i] = thread;
        }

        assertJoinable(threads);

        int upTotal = map.get(upKey);
        int downTotal = map.get(downKey);

        assertTrue("concurrent access to locked code caused wrong total", upTotal + downTotal == 0);
    }

    private static class MapTryLockThread extends TestHelper {

        public MapTryLockThread(IMap<String, Integer> map, String upKey, String downKey) {
            super(map, upKey, downKey);
        }

        public void doRun() throws Exception {
            if (map.tryLock(upKey)) {
                try {
                    if (map.tryLock(downKey)) {
                        try {
                            work();
                        } finally {
                            map.unlock(downKey);
                        }
                    }
                } finally {
                    map.unlock(upKey);
                }
            }
        }
    }

    private static class MapTryLockTimeOutThread extends TestHelper {

        public MapTryLockTimeOutThread(IMap<String, Integer> map, String upKey, String downKey) {
            super(map, upKey, downKey);
        }

        public void doRun() throws Exception {
            if (map.tryLock(upKey, 1, TimeUnit.MILLISECONDS)) {
                try {
                    if (map.tryLock(downKey, 1, TimeUnit.MILLISECONDS)) {
                        try {
                            work();
                        } finally {
                            map.unlock(downKey);
                        }
                    }
                } finally {
                    map.unlock(upKey);
                }
            }
        }
    }

    private static abstract class TestHelper extends Thread {
        protected static final int ITERATIONS = 1000 * 10;
        protected final Random random = new Random();
        protected final IMap<String, Integer> map;
        protected final String upKey;
        protected final String downKey;

        public TestHelper(IMap<String, Integer> map, String upKey, String downKey) {
            this.map = map;
            this.upKey = upKey;
            this.downKey = downKey;
        }

        public void run() {
            try {
                for (int i = 0; i < ITERATIONS; i++) {
                    doRun();
                }
            } catch (Exception e) {
                throw new RuntimeException("Test Thread crashed with ", e);
            }
        }

        abstract void doRun() throws Exception;

        public void work() {
            int upTotal = map.get(upKey);
            int downTotal = map.get(downKey);

            int dif = random.nextInt(1000);
            upTotal += dif;
            downTotal -= dif;

            map.put(upKey, upTotal);
            map.put(downKey, downTotal);
        }
    }
}
