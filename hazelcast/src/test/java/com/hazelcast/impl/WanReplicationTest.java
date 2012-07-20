/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.impl.wan.WanMergeListener;
import com.hazelcast.merge.PassThroughMergePolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.impl.TestUtil.getConcurrentMapManager;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class WanReplicationTest {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        Hazelcast.shutdownAll();
    }

    @After
    @Before
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testWANClustering() throws Exception {
        Config c1 = new Config();
        Config c2 = new Config();
        c1.getGroupConfig().setName("newyork");
        c1.addWanReplicationConfig(new WanReplicationConfig()
                .setName("my-wan")
                .addTargetClusterConfig(new WanTargetClusterConfig()
                        .addEndpoint("127.0.0.1:5702").setGroupName("london")));
        c1.getMapConfig("default").setWanReplicationRef(new WanReplicationRef()
                .setName("my-wan")
                .setMergePolicy(PassThroughMergePolicy.NAME));
        c2.getGroupConfig().setName("london");
        c2.addWanReplicationConfig(new WanReplicationConfig()
                .setName("my-wan")
                .addTargetClusterConfig(new WanTargetClusterConfig()
                        .addEndpoint("127.0.0.1:5701").setGroupName("newyork")));
        c2.getMapConfig("default").setWanReplicationRef(new WanReplicationRef()
                .setName("my-wan")
                .setMergePolicy(PassThroughMergePolicy.NAME));
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
        HazelcastInstance h12 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h13 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h22 = Hazelcast.newHazelcastInstance(c2);
        int size = 100;
        MergeLatch mergeLatch1 = new MergeLatch(2 * size);
        MergeLatch mergeLatch2 = new MergeLatch(size);
        getConcurrentMapManager(h1).addWanMergeListener(mergeLatch1);
        getConcurrentMapManager(h12).addWanMergeListener(mergeLatch1);
        getConcurrentMapManager(h13).addWanMergeListener(mergeLatch1);
        getConcurrentMapManager(h2).addWanMergeListener(mergeLatch2);
        getConcurrentMapManager(h22).addWanMergeListener(mergeLatch2);
        for (int i = 0; i < size; i++) {
            h2.getMap("default").put(i, "value" + i);
            h22.getMap("default").put(size + i, "value" + (size + i));
        }
        assertTrue("Latch state 1: " + mergeLatch1, mergeLatch1.await(60, TimeUnit.SECONDS));
        Thread.sleep(5000);
        assertEquals(0, mergeLatch2.totalOperations());
        assertEquals(2 * size, mergeLatch1.getUpdateCount());
        assertEquals(2 * size, mergeLatch1.totalOperations());
        assertEquals(2 * size, h2.getMap("default").size());
        assertEquals(2 * size, h1.getMap("default").size());
        assertEquals(2 * size, h12.getMap("default").size());
        assertEquals(2 * size, h13.getMap("default").size());
        assertEquals(2 * size, h22.getMap("default").size());
        mergeLatch1.reset();
        for (int i = 0; i < size / 2; i++) {
            h1.getMap("default").remove(i);
            h13.getMap("default").remove(size + i);
        }
        assertTrue("Latch state 2: " + mergeLatch2, mergeLatch2.await(60, TimeUnit.SECONDS));
        Thread.sleep(5000);
        assertEquals(size, mergeLatch2.getRemoveCount());
        assertEquals(size, mergeLatch2.totalOperations());
        assertEquals(0, mergeLatch1.totalOperations());
        assertEquals(size, h1.getMap("default").size());
        assertEquals(size, h2.getMap("default").size());
        assertEquals(size, h12.getMap("default").size());
        assertEquals(size, h13.getMap("default").size());
        assertEquals(size, h22.getMap("default").size());
    }

    @Test
    public void testWANClustering2() throws Exception {
        Config c1 = new Config();
        Config c2 = new Config();
        c1.getGroupConfig().setName("newyork");
        c1.addWanReplicationConfig(new WanReplicationConfig()
                .setName("my-wan")
                .addTargetClusterConfig(new WanTargetClusterConfig()
                        .addEndpoint("127.0.0.1:5703").setGroupName("london")));
        c1.getMapConfig("default").setWanReplicationRef(new WanReplicationRef()
                .setName("my-wan")
                .setMergePolicy(PassThroughMergePolicy.NAME));
        c2.getGroupConfig().setName("london");
        c2.addWanReplicationConfig(new WanReplicationConfig()
                .setName("my-wan")
                .addTargetClusterConfig(new WanTargetClusterConfig()
                        .addEndpoint("127.0.0.1:5701").setGroupName("newyork")));
        c2.getMapConfig("default").setWanReplicationRef(new WanReplicationRef()
                .setName("my-wan")
                .setMergePolicy(PassThroughMergePolicy.NAME));
        HazelcastInstance h10 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h11 = Hazelcast.newHazelcastInstance(c1);
        int size = 1000;
        HazelcastInstance h20 = Hazelcast.newHazelcastInstance(c2);
        HazelcastInstance h21 = Hazelcast.newHazelcastInstance(c2);
        HazelcastInstance h12 = Hazelcast.newHazelcastInstance(c1);
        MergeLatch mergeLatch1 = new MergeLatch(size);
        getConcurrentMapManager(h10).addWanMergeListener(mergeLatch1);
        getConcurrentMapManager(h11).addWanMergeListener(mergeLatch1);
        getConcurrentMapManager(h12).addWanMergeListener(mergeLatch1);
        MergeLatch mergeLatch2 = new MergeLatch(size);
        getConcurrentMapManager(h20).addWanMergeListener(mergeLatch2);
        getConcurrentMapManager(h21).addWanMergeListener(mergeLatch2);
        for (int i = 0; i < size; i++) {
            h11.getMap("default").put(i, "value" + i);
        }
        assertTrue("Latch state: " + mergeLatch2, mergeLatch2.await(60, TimeUnit.SECONDS));
        Thread.sleep(5000);
        assertEquals(size, mergeLatch2.totalOperations());
        assertEquals(0, mergeLatch1.totalOperations());
        assertEquals(size, h10.getMap("default").size());
        assertEquals(size, h20.getMap("default").size());
        assertEquals(size, h12.getMap("default").size());
        assertEquals(size, h11.getMap("default").size());
        assertEquals(size, h21.getMap("default").size());
        mergeLatch2.reset();
        for (int i = 0; i < size; i++) {
            h21.getMap("default").put(size + i, "value" + (size + i));
        }
        assertTrue("Latch state: " + mergeLatch1, mergeLatch1.await(60, TimeUnit.SECONDS));
        Thread.sleep(5000);
        assertEquals(size, mergeLatch1.totalOperations());
        assertEquals(0, mergeLatch2.totalOperations());
        assertEquals(2 * size, h10.getMap("default").size());
        assertEquals(2 * size, h20.getMap("default").size());
        assertEquals(2 * size, h12.getMap("default").size());
        assertEquals(2 * size, h11.getMap("default").size());
        assertEquals(2 * size, h21.getMap("default").size());
        mergeLatch1.reset(size / 2);
        mergeLatch2.reset(size / 2);
        for (int i = 0; i < size / 2; i++) {
            h10.getMap("default").remove(i);
            h21.getMap("default").remove(size + i);
        }
        assertTrue("Latch state: " + mergeLatch1, mergeLatch1.await(60, TimeUnit.SECONDS));
        assertTrue("Latch state: " + mergeLatch2, mergeLatch2.await(60, TimeUnit.SECONDS));
        Thread.sleep(5000);
        assertEquals(size / 2, mergeLatch1.totalOperations());
        assertEquals(size / 2, mergeLatch2.totalOperations());
        assertEquals(size, h10.getMap("default").size());
        assertEquals(size, h20.getMap("default").size());
        assertEquals(size, h12.getMap("default").size());
        assertEquals(size, h11.getMap("default").size());
        assertEquals(size, h21.getMap("default").size());
    }

    @Test
    public void testWANClusteringActivePassive() throws Exception {
        Config c1 = new Config();
        Config c2 = new Config();
        c1.getGroupConfig().setName("newyork");
        c1.addWanReplicationConfig(new WanReplicationConfig()
                .setName("my-wan")
                .addTargetClusterConfig(new WanTargetClusterConfig()
                        .addEndpoint("127.0.0.1:5702").setGroupName("london")));
        c1.getMapConfig("default").setWanReplicationRef(new WanReplicationRef()
                .setName("my-wan")
                .setMergePolicy(PassThroughMergePolicy.NAME));
        c2.getGroupConfig().setName("london");
        c2.getMapConfig("default").setWanReplicationRef(new WanReplicationRef()
                .setName("my-wan")
                .setMergePolicy(PassThroughMergePolicy.NAME));
        HazelcastInstance h10 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h20 = Hazelcast.newHazelcastInstance(c2);
        int size = 1000;
        MergeLatch mergeLatch2 = new MergeLatch(size);
        getConcurrentMapManager(h20).addWanMergeListener(mergeLatch2);
        for (int i = 0; i < size; i++) {
            h10.getMap("default").put(i, "value" + i);
        }
        assertTrue("Latch state: " + mergeLatch2, mergeLatch2.await(60, TimeUnit.SECONDS));
        Thread.sleep(5000);
        assertEquals(size, mergeLatch2.totalOperations());
        assertEquals(size, h10.getMap("default").size());
        assertEquals(size, h20.getMap("default").size());
        for (int i = 0; i < size; i++) {
            assertEquals("value" + i, h20.getMap("default").get(i));
        }
    }

    class MergeLatch implements WanMergeListener {
        final AtomicInteger removeCount = new AtomicInteger();
        final AtomicInteger updateCount = new AtomicInteger();
        final AtomicInteger ignoreCount = new AtomicInteger();
        final AtomicReference<CountDownLatch> latch;

        MergeLatch(int count) {
            latch = new AtomicReference<CountDownLatch>(new CountDownLatch(count));
        }

        public long getCount() {
            return latch.get().getCount();
        }

        public void countDown() {
            CountDownLatch l = latch.get();
            if (l != null) {
                l.countDown();
            }
        }

        public void entryRemoved() {
            removeCount.incrementAndGet();
            countDown();
        }

        public void entryUpdated() {
            updateCount.incrementAndGet();
            countDown();
        }

        public void entryIgnored() {
            ignoreCount.incrementAndGet();
            countDown();
        }

        public int totalOperations() {
            return getIgnoreCount() + getRemoveCount() + getUpdateCount();
        }

        public int getRemoveCount() {
            return removeCount.get();
        }

        public int getUpdateCount() {
            return updateCount.get();
        }

        public int getIgnoreCount() {
            return ignoreCount.get();
        }

        public void reset() {
            removeCount.set(0);
            updateCount.set(0);
            ignoreCount.set(0);
        }

        public void reset(int count) {
            latch.set(new CountDownLatch(count));
            reset();
        }

        public boolean await(int time, TimeUnit timeUnit) throws InterruptedException {
            return latch.get().await(time, timeUnit);
        }

        @Override
        public String toString() {
            return "MergeLatch{" +
                    "count=" + getCount() +
                    ", removeCount=" + removeCount.get() +
                    ", updateCount=" + updateCount.get() +
                    ", ignoreCount=" + ignoreCount.get() +
                    '}';
        }
    }
}
