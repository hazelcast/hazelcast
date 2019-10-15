/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapPutAllWithBouncingMemberTest extends HazelcastTestSupport {

    private static final int MAP_SIZE = 1000;
    private static final int DURATION_SECONDS = 30;

    private TestHazelcastInstanceFactory factory;
    private Config config;

    @Before
    public void setUp() {
        factory = createHazelcastInstanceFactory();
        config = getConfig();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testPutAll_whenAddingAndTerminatingMembers_thenPutAllShouldNotFail() {
        testPutAll();
    }

    @Test
    public void testPutAll_whenAddingAndTerminatingMembers_thenPutAllShouldNotFail_withBatching() {
        config.setProperty("hazelcast.map.put.all.batch.size", "2");

        testPutAll();
    }

    private void testPutAll() {
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        final IMap<Integer, Integer> map = instance.getMap(randomMapName());

        final AtomicBoolean done = new AtomicBoolean(false);
        Thread bouncingThread = new Thread() {
            public void run() {
                while (!done.get()) {
                    HazelcastInstance newInstance = factory.newHazelcastInstance(config);
                    sleepSeconds(5);
                    factory.terminate(newInstance);
                }
            }
        };
        bouncingThread.start();

        HashMap<Integer, Integer> batch = new HashMap<Integer, Integer>();
        for (int i = 0; i < MAP_SIZE; i++) {
            batch.put(i, i);
        }

        long started = System.nanoTime();
        long elapsed;
        int i = 0;
        do {
            map.clear();
            map.putAll(batch);
            sleepMillis(3);

            elapsed = NANOSECONDS.toSeconds(System.nanoTime() - started);
            if (++i % 500 == 0) {
                System.out.println(elapsed + " sec (" + i + " iterations)");
            }
        } while (elapsed < DURATION_SECONDS);
        System.out.println(elapsed + " sec (" + i + " iterations)");

        done.set(true);
        assertJoinable(bouncingThread);

        assertEquals("The map size should be " + MAP_SIZE, MAP_SIZE, map.size());
        assertTrue("There should have been multiple iterations, but was " + i, i > 1);
    }
}
