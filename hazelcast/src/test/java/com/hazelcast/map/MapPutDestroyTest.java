package com.hazelcast.map;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class MapPutDestroyTest extends HazelcastTestSupport {
    @Test
    public void testConcurrentPutDestroy_doesNotCauseNPE() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance instance = factory.newHazelcastInstance();
        final String mapName = randomString();
        final AtomicReference<Throwable> error = new AtomicReference<Throwable>(null);

        final AtomicBoolean stop = new AtomicBoolean();

        Thread t1 = new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            while (!stop.get()) {
                                IMap<Object, Object> map = instance.getMap(mapName);
                                map.put(System.currentTimeMillis(), Boolean.TRUE);
                            }
                        } catch (Throwable e) {
                            error.set(e);
                        }
                    }
                }
        );

        Thread t2 = new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            while (!stop.get()) {
                                IMap<Object, Object> map = instance.getMap(mapName);
                                map.destroy();
                            }
                        } catch (Throwable e) {
                            error.set(e);
                        }
                    }
                }
        );

        t1.start();
        t2.start();

        sleepSeconds(10);
        stop.set(true);

        try {
            t1.join();
        } catch (Throwable e) {
            e.printStackTrace();
        }

        try {
            t2.join();
        } catch (Throwable e) {
            e.printStackTrace();
        }

        Throwable object = error.get();

        if (object != null) {
            object.printStackTrace(System.out);
            fail(object.getMessage());
        }
    }
}