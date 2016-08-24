package com.hazelcast.map.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.proxy.NearCachedMapProxyImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static java.lang.Integer.parseInt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * A test to ensure no lost invalidations on the Near Cache.
 *
 * Issue: https://github.com/hazelcast/hazelcast/issues/4671
 *
 * Thansk Lukas Blunschi for this test (https://github.com/lukasblu).
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class NearCacheStaleReadTest extends HazelcastTestSupport {

    private static final int NUM_GETTERS = 7;
    private static final int MAX_RUNTIME = 30;
    private static final String KEY = "key123";
    private static final String MAP_NAME = "testMap" + NearCacheStaleReadTest.class.getSimpleName();

    private static final Logger LOGGER = Logger.getLogger(NearCacheStaleReadTest.class);

    private IMap<String, String> map;

    private AtomicInteger valuePut = new AtomicInteger(0);

    private AtomicBoolean stop = new AtomicBoolean(false);

    private AtomicInteger assertionViolationCount = new AtomicInteger(0);

    private AtomicBoolean failed = new AtomicBoolean(false);

    @Test
    public void testNoLostInvalidationsEventually() throws Exception {
        assertionViolationCount.set(0);
        testNoLostInvalidations(false);
    }

    private void testNoLostInvalidations(boolean strict) throws Exception {
        // create hazelcast config
        Config config = getConfig();
        config.setProperty("hazelcast.logging.type", "log4j");

        // we use a single node only - do not search for others
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        // configure Near Cache
        NearCacheConfig nearCacheConfig = getNearCacheConfig();

        // enable Near Cache
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.setNearCacheConfig(nearCacheConfig);

        // create Hazelcast instance
        HazelcastInstance hzInstance = Hazelcast.newHazelcastInstance(config);

        // get hazelcast map
        map = hzInstance.getMap(MAP_NAME);

        // run test
        runTestInternal();

        // test eventually consistent
        Thread.sleep(5000);
        int valuePutLast = valuePut.get();
        String valueMapStr = map.get(KEY);
        int valueMap = parseInt(valueMapStr);

        // fail if not eventually consistent
        String msg = null;
        if (valueMap < valuePutLast) {
            msg = "Near Cache did *not* become consistent. (valueMap = " + valueMap + ", valuePut = " + valuePutLast + ").";

            // flush Near Cache and re-fetch value
            ((NearCachedMapProxyImpl) map).getNearCache().clear();
            String valueMap2Str = map.get(KEY);
            int valueMap2 = parseInt(valueMap2Str);

            // test again
            if (valueMap2 < valuePutLast) {
                msg += " Unexpected inconsistency! (valueMap2 = " + valueMap2 + ", valuePut = " + valuePutLast + ").";
            } else {
                msg += " Flushing the Near Cache cleared the inconsistency. (valueMap2 = " + valueMap2
                        + ", valuePut = " + valuePutLast + ").";
            }
        }

        // stop hazelcast
        hzInstance.getLifecycleService().terminate();

        // fail after stopping hazelcast instance
        if (msg != null) {
            LOGGER.warn(msg);
            fail(msg);
        }

        // fail if strict is required and assertion was violated
        if (strict && assertionViolationCount.get() > 0) {
            msg = "Assertion violated " + assertionViolationCount.get() + " times.";
            LOGGER.warn(msg);
            fail(msg);
        }
    }

    protected NearCacheConfig getNearCacheConfig() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        // this enables caching of local entries
        nearCacheConfig.setCacheLocalEntries(true);
        nearCacheConfig.setInMemoryFormat(OBJECT);
        return nearCacheConfig;
    }

    private void runTestInternal() throws Exception {
        // start 1 putter thread (put0)
        Thread threadPut = new Thread(new PutRunnable(), "put0");
        threadPut.start();

        // wait for putter thread to start before starting getter threads
        sleepMillis(300);

        // start numGetters getter threads (get0-numGetters)
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < NUM_GETTERS; i++) {
            Thread thread = new Thread(new GetRunnable(), "get" + i);
            threads.add(thread);
        }
        for (Thread thread : threads) {
            thread.start();
        }

        // stop after maxRuntime seconds
        int i = 0;
        while (!stop.get() && i++ < MAX_RUNTIME) {
            sleepMillis(1000);
        }
        if (!stop.get()) {
            LOGGER.info("Problem did not occur within " + MAX_RUNTIME + "s.");
        }
        stop.set(true);
        threadPut.join();
        for (Thread thread : threads) {
            thread.join();
        }
    }

    private class PutRunnable implements Runnable {

        @Override
        public void run() {
            LOGGER.info(Thread.currentThread().getName() + " started.");
            int i = 0;
            while (!stop.get()) {
                i++;

                // put new value and update last state
                // note: the value in the map/Near Cache is *always* larger or equal to valuePut
                // assertion: valueMap >= valuePut
                map.put(KEY, String.valueOf(i));
                valuePut.set(i);

                // check if we see our last update
                String valueMapStr = map.get(KEY);
                if (valueMapStr == null) {
                    continue;
                }
                int valueMap = parseInt(valueMapStr);
                if (valueMap != i) {
                    assertionViolationCount.incrementAndGet();
                    LOGGER.warn("Assertion violated! (valueMap = " + valueMap + ", i = " + i + ")");

                    // sleep to ensure Near Cache invalidation is really lost
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOGGER.warn("Interrupted: " + e.getMessage());
                    }

                    // test again and stop if really lost
                    valueMapStr = map.get(KEY);
                    valueMap = parseInt(valueMapStr);
                    if (valueMap != i) {
                        LOGGER.warn("Near Cache invalidation lost! (valueMap = " + valueMap + ", i = " + i + ")");
                        failed.set(true);
                        stop.set(true);
                    }
                }
            }
            LOGGER.info(Thread.currentThread().getName() + " performed " + i + " operations.");
        }

    }

    private class GetRunnable implements Runnable {

        @Override
        public void run() {
            LOGGER.info(Thread.currentThread().getName() + " started.");
            int n = 0;
            while (!stop.get()) {
                n++;

                // blindly get the value (to trigger the issue) and parse the value (to get some CPU load)
                String valueMapStr = map.get(KEY);
                int i = parseInt(valueMapStr);
                assertEquals("" + i, valueMapStr);
            }
            LOGGER.info(Thread.currentThread().getName() + " performed " + n + " operations.");
        }
    }
}
