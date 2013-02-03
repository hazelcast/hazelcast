/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.util.RandomBlockJUnit4ClassRunner;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(RandomBlockJUnit4ClassRunner.class)
public class ListenerLifecycleTest {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    /**
     * See <code>{@link MapEntryListenerTest#createAfterDestroyListenerTest()}</code>
     */
    public void testListenerLifecycle() throws InterruptedException {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(new Config());
        final String name = "testListenerLifecycle";
        final int sleep = 100;
        // IMap
        IMap map = hz.getMap(name);
        final CountDownLatch mapLatch = new CountingCountdownLatch(2);
        final EntryListener el = new EntryAdapter() {
            public void entryAdded(EntryEvent event) {
                mapLatch.countDown();
            }
        };
        map.addEntryListener(el, false);
        map.put(1, 1);
        Thread.sleep(sleep);
        map.destroy();
        map = hz.getMap(name);
        map.addEntryListener(el, false);
        map.put(2, 2);
        Thread.sleep(sleep);
        map.removeEntryListener(el);
        map.put(3, 3);
        Thread.sleep(sleep);
        assertTrue("Remaining:" + mapLatch.getCount(), mapLatch.await(3, TimeUnit.SECONDS));
        map.destroy();
        // IQueue
        IQueue q = hz.getQueue(name);
        final CountDownLatch qLatch = new CountingCountdownLatch(2);
        final ItemListener ql = new ItemListener() {
            public void itemAdded(ItemEvent itemEvent) {
                qLatch.countDown();
            }

            public void itemRemoved(ItemEvent itemEvent) {
            }
        };
        q.addItemListener(ql, false);
        q.offer(1);
        Thread.sleep(sleep);
        q.destroy();
        q = hz.getQueue(name);
        q.addItemListener(ql, false);
        q.offer(2);
        Thread.sleep(sleep);
        q.removeItemListener(ql);
        q.offer(3);
        Thread.sleep(sleep);
        assertTrue("Remaining:" + qLatch.getCount(), qLatch.await(3, TimeUnit.SECONDS));
        q.destroy();
        // ITopic
        ITopic t = hz.getTopic(name);
        final CountDownLatch tLatch = new CountingCountdownLatch(2);
        final MessageListener ml = new MessageListener() {
            public void onMessage(Message message) {
                tLatch.countDown();
            }
        };
        t.addMessageListener(ml);
        t.publish(1);
        Thread.sleep(sleep);
        t.destroy();
        t = hz.getTopic(name);
        t.addMessageListener(ml);
        t.publish(2);
        Thread.sleep(sleep);
        t.removeMessageListener(ml);
        t.publish(3);
        Thread.sleep(sleep);
        assertTrue("Remaining:" + tLatch.getCount(), tLatch.await(3, TimeUnit.SECONDS));
        t.destroy();
        // MultiMap
        MultiMap mmap = hz.getMultiMap(name);
        final CountDownLatch mmapLatch = new CountingCountdownLatch(2);
        final EntryListener el2 = new EntryAdapter() {
            public void entryAdded(EntryEvent event) {
                mmapLatch.countDown();
            }
        };
        mmap.addEntryListener(el2, false);
        mmap.put(1, 1);
        Thread.sleep(sleep);
        mmap.destroy();
        mmap = hz.getMultiMap(name);
        mmap.addEntryListener(el2, false);
        mmap.put(2, 2);
        Thread.sleep(sleep);
        mmap.removeEntryListener(el2);
        mmap.put(3, 3);
        Thread.sleep(sleep);
        assertTrue("Remaining:" + mmapLatch.getCount(), mmapLatch.await(3, TimeUnit.SECONDS));
        mmap.destroy();
        // IList
        IList l = hz.getList(name);
        final CountDownLatch lLatch = new CountingCountdownLatch(2);
        final ItemListener ll = new ItemListener() {
            public void itemAdded(ItemEvent itemEvent) {
                lLatch.countDown();
            }

            public void itemRemoved(ItemEvent itemEvent) {
            }
        };
        l.addItemListener(ll, false);
        l.add(1);
        Thread.sleep(sleep);
        l.destroy();
        l = hz.getList(name);
        l.addItemListener(ll, false);
        l.add(2);
        Thread.sleep(sleep);
        l.removeItemListener(ll);
        l.add(3);
        Thread.sleep(sleep);
        assertTrue("Remaining:" + lLatch.getCount(), lLatch.await(3, TimeUnit.SECONDS));
        l.destroy();
        // ISet
        ISet s = hz.getSet(name);
        final CountDownLatch sLatch = new CountingCountdownLatch(2);
        final ItemListener sl = new ItemListener() {
            public void itemAdded(ItemEvent itemEvent) {
                sLatch.countDown();
            }

            public void itemRemoved(ItemEvent itemEvent) {
            }
        };
        s.addItemListener(sl, false);
        s.add(1);
        Thread.sleep(sleep);
        s.destroy();
        s = hz.getSet(name);
        s.addItemListener(sl, false);
        s.add(2);
        Thread.sleep(sleep);
        s.removeItemListener(sl);
        s.add(3);
        Thread.sleep(sleep);
        assertTrue("Remaining:" + sLatch.getCount(), sLatch.await(3, TimeUnit.SECONDS));
        s.destroy();
    }

    @Test
    public void testConfigListenerInitialization() throws InterruptedException {
        Config config = new Config();
        config.addListenerConfig(new ListenerConfig(new CountdownMembershipListener()));
        final InstanceListener instanceListener = new CountdownInstanceListener();
        config.addListenerConfig(new ListenerConfig(instanceListener));
        final String configName = "testConfigListenerInitialization-";
        MapConfig mapConfig = config.getMapConfig(configName + "*");
        mapConfig.addEntryListenerConfig(new EntryListenerConfig(new CountdownEntryListener(), false, true));
        QueueConfig queueConfig = config.getQueueConfig(configName + "*");
        queueConfig.addItemListenerConfig(new ItemListenerConfig(new CountdownItemListener(), true));
        TopicConfig topicConfig = config.getTopicConfig(configName + "*");
        topicConfig.addMessageListenerConfig(new ListenerConfig(new CountdownMessageListener()));
        MultiMapConfig multiMapConfig = config.getMultiMapConfig(configName + "*");
        multiMapConfig.addEntryListenerConfig(new EntryListenerConfig(new CountdownMultimapEntryListener(), false, false));
        HazelcastInstance hz = null;
        for (int i = 0; i < CountdownMembershipListener.MEMBERS; i++) {
            hz = Hazelcast.newHazelcastInstance(config);
        }
        assertTrue(CountdownMembershipListener.LATCH.await(5, TimeUnit.SECONDS));
        for (int i = 0; i < CountdownInstanceListener.INSTANCES; i++) {
            Map map = hz.getMap(configName + i);
            for (int j = 0; j < CountdownEntryListener.ENTRIES; j++) {
                map.put(j, j);
            }
            MultiMap mm = hz.getMultiMap(configName + i);
            for (int j = 0; j < CountdownMultimapEntryListener.ENTRIES; j++) {
                mm.put(j, j);
            }
            Queue q = hz.getQueue(configName + i);
            for (int j = 0; j < CountdownItemListener.ENTRIES; j++) {
                q.offer(j);
            }
            ITopic t = hz.getTopic(configName + i);
            for (int j = 0; j < CountdownMessageListener.MESSAGES; j++) {
                t.publish(j);
            }
        }
        assertTrue(CountdownInstanceListener.LATCH.await(5, TimeUnit.SECONDS));
        assertTrue("Remaining: " + CountdownEntryListener.LATCH.getCount(),
                CountdownEntryListener.LATCH.await(5, TimeUnit.SECONDS));
        assertTrue("Remaining: " + CountdownMultimapEntryListener.LATCH.getCount(),
                CountdownMultimapEntryListener.LATCH.await(5, TimeUnit.SECONDS));
        assertTrue("Remaining: " + CountdownItemListener.LATCH.getCount(),
                CountdownItemListener.LATCH.await(5, TimeUnit.SECONDS));
        assertTrue("Remaining: " + CountdownMessageListener.LATCH.getCount(),
                CountdownMessageListener.LATCH.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testRemoveListener() throws InterruptedException {
        final AtomicInteger counter = new AtomicInteger();
        final int k = 5;
        final String name = "test";
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(null);

        IMap map = hazelcast.getMap(name);
        map.addEntryListener(new NamedEntryListener(name, counter), true);
        for (int i = 0; i < k; i++) {
            map.put(i, i);
        }
        Thread.sleep(1000);
        assertEquals(k, counter.get());
        map.removeEntryListener(new NamedEntryListener(name));
        map.put(k, k);
        Thread.sleep(500);
        assertEquals(k, counter.get());
        counter.set(0);

        ITopic t = hazelcast.getTopic(name);
        t.addMessageListener(new NamedMessageListener(name, counter));
        for (int i = 0; i < k; i++) {
            t.publish(i);
        }
        Thread.sleep(1000);
        assertEquals(k, counter.get());
        t.removeMessageListener(new NamedMessageListener(name));
        t.publish(k);
        Thread.sleep(500);
        assertEquals(k, counter.get());
        counter.set(0);

        IQueue q = hazelcast.getQueue(name);
        q.addItemListener(new NamedItemListener(name, counter), true);
        for (int i = 0; i < k; i++) {
            q.offer(i);
        }
        Thread.sleep(1000);
        assertEquals(k, counter.get());
        q.removeItemListener(new NamedItemListener(name));
        q.offer(k);
        Thread.sleep(500);
        assertEquals(k, counter.get());
        counter.set(0);

    }

    static class CountdownMembershipListener implements MembershipListener {
        static final int MEMBERS = 3;
        static final int EVENT_TOTAL = 6;

        static final CountDownLatch LATCH = new CountingCountdownLatch(EVENT_TOTAL);

        public void memberAdded(MembershipEvent membershipEvent) {
            LATCH.countDown();
        }

        public void memberRemoved(MembershipEvent membershipEvent) {
        }
    }

    static class CountdownInstanceListener implements InstanceListener {
        static final int INSTANCES = 5;
        static final int EVENT_TOTAL = INSTANCES * CountdownMembershipListener.MEMBERS * 5; // map, multimap, topic, queue x2
        static final CountDownLatch LATCH = new CountingCountdownLatch(EVENT_TOTAL);

        public void instanceCreated(InstanceEvent event) {
            LATCH.countDown();
        }

        public void instanceDestroyed(InstanceEvent event) {
        }
    }

    static class CountdownEntryListener extends EntryAdapter {
        static final int ENTRIES = 15;
        static final int EVENT_TOTAL = CountdownMembershipListener.MEMBERS * CountdownInstanceListener.INSTANCES * ENTRIES;
        static final CountDownLatch LATCH = new CountingCountdownLatch(EVENT_TOTAL);

        public void entryAdded(EntryEvent event) {
            LATCH.countDown();
        }
    }

    public static class CountdownMultimapEntryListener extends EntryAdapter {
        static final int ENTRIES = 15;
        static final int EVENT_TOTAL = CountdownMembershipListener.MEMBERS * CountdownInstanceListener.INSTANCES * ENTRIES;
        static final CountDownLatch LATCH = new CountingCountdownLatch(EVENT_TOTAL);

        public void entryAdded(EntryEvent event) {
            LATCH.countDown();
        }
    }

    static class CountdownItemListener implements ItemListener {
        static final int ENTRIES = 15;
        static final int EVENT_TOTAL = CountdownMembershipListener.MEMBERS * CountdownInstanceListener.INSTANCES * ENTRIES;
        static final CountDownLatch LATCH = new CountingCountdownLatch(EVENT_TOTAL);

        public void itemAdded(ItemEvent itemEvent) {
            LATCH.countDown();
        }

        public void itemRemoved(ItemEvent itemEvent) {
        }
    }

    static class CountdownMessageListener implements MessageListener {
        static final int MESSAGES = 15;
        static final int EVENT_TOTAL = CountdownMembershipListener.MEMBERS * CountdownInstanceListener.INSTANCES * MESSAGES;
        static final CountDownLatch LATCH = new CountingCountdownLatch(EVENT_TOTAL);

        public void onMessage(Message message) {
            LATCH.countDown();
        }
    }

    static class CountingCountdownLatch extends CountDownLatch {
        final AtomicInteger count;

        public CountingCountdownLatch(int count) {
            super(count);
            this.count = new AtomicInteger(count);
        }

        public void countDown() {
            if (count.decrementAndGet() < 0) {
                fail("Countdown to negative!");
            }
            super.countDown();
        }

        public void await() throws InterruptedException {
            super.await();
            checkCount();
        }

        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            if (super.await(timeout, unit)) {
                checkCount();
                return true;
            }
            return false;
        }

        void checkCount() {
            int c;
            if ((c = count.get()) < 0) {
                fail("Countdown to negative = " + c);
            }
        }
    }

    private static abstract class NamedListener {
        String name;
        final AtomicInteger counter;

        NamedListener(final String name) {
            this(name, new AtomicInteger());
        }

        NamedListener(final String name, final AtomicInteger counter) {
            this.name = name;
            this.counter = counter;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final NamedListener that = (NamedListener) o;

            if (name != null ? !name.equals(that.name) : that.name != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }
    }

    static class NamedEntryListener extends NamedListener implements EntryListener {

        NamedEntryListener(final String name) {
            super(name);
        }

        NamedEntryListener(final String name, final AtomicInteger counter) {
            super(name, counter);
        }

        public void entryAdded(final EntryEvent event) {
            counter.incrementAndGet();
        }
        public void entryRemoved(final EntryEvent event) {}
        public void entryUpdated(final EntryEvent event) {}
        public void entryEvicted(final EntryEvent event) {}
    }

    static class NamedMessageListener extends NamedListener implements MessageListener {

        NamedMessageListener(final String name) {
            super(name);
        }

        NamedMessageListener(final String name, final AtomicInteger counter) {
            super(name, counter);
        }

        public void onMessage(final Message message) {
            counter.incrementAndGet();
        }
    }

    static class NamedItemListener extends NamedListener implements ItemListener {

        NamedItemListener(final String name) {
            super(name);
        }

        NamedItemListener(final String name, final AtomicInteger counter) {
            super(name, counter);
        }

        public void itemAdded(final ItemEvent item) {
            counter.incrementAndGet();
        }
        public void itemRemoved(final ItemEvent item) {
        }
    }
}
