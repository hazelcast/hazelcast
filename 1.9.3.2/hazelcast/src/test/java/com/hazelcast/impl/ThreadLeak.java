/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadLeak {
    @Test
    public void testThreadLeak() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final IMap map = h1.getMap("default");
        Config superConfig = new Config();
        superConfig.setSuperClient(true);
        HazelcastInstance hSuper = Hazelcast.newHazelcastInstance(superConfig);
        final IMap smap = hSuper.getMap("default");
        final AtomicLong counter = new AtomicLong();
        smap.addEntryListener(new EntryListener() {
            public void entryAdded(EntryEvent objectObjectEntryEvent) {
            }

            public void entryRemoved(EntryEvent objectObjectEntryEvent) {
            }

            public void entryUpdated(EntryEvent objectObjectEntryEvent) {
//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                counter.incrementAndGet();
            }

            public void entryEvicted(EntryEvent objectObjectEntryEvent) {
            }
        }, false);
        final int THREAD_COUNT = 10;
        final int STATS_SECONDS = 5;
        final Random random = new Random();
        ExecutorService es = Executors.newFixedThreadPool(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            es.submit(new Runnable() {
                public void run() {
                    while (true) {
                        map.put(random.nextInt(10), 1);
                    }
                }
            });
        }
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(STATS_SECONDS * 1000);
                        System.out.println("counter " + counter.getAndSet(0));
                    } catch (InterruptedException ignored) {
                        return;
                    }
                }
            }
        });
        Thread.sleep(10000000);
    }
}
