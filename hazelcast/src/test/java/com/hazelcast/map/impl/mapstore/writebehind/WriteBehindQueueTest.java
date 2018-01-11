/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.ReachedMaxSizeException;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntries;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueues.createBoundedWriteBehindQueue;
import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueues.createDefaultWriteBehindQueue;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WriteBehindQueueTest extends HazelcastTestSupport {

    @Test
    public void smoke() {
        final WriteBehindQueue<DelayedEntry> queue = createWBQ();

        assertEquals(0, queue.size());
    }

    @Test
    public void testAddEnd() {
        WriteBehindQueue<DelayedEntry> queue = createWBQ();
        addEnd(1000, queue);

        assertEquals(1000, queue.size());
    }

    @Test
    public void testAddFront() {
        WriteBehindQueue<DelayedEntry> queue = createWBQ();
        List<DelayedEntry> delayedEntries = createDelayedEntryList(1000);
        queue.addFirst(delayedEntries);

        assertEquals(1000, queue.size());
    }

    @Test(expected = ReachedMaxSizeException.class)
    public void testWBQMaxSizeException() {
        final WriteBehindQueue<DelayedEntry> queue = createBoundedWBQ();
        // put total 1001 items. Max allowed is 1000
        addEnd(1001, queue);
    }

    @Test(expected = ReachedMaxSizeException.class)
    public void testWBQMaxSizeException_withMultipleWBQ() {
        final AtomicInteger counter = new AtomicInteger(0);
        final WriteBehindQueue<DelayedEntry> queue1 = createBoundedWBQ(counter);
        final WriteBehindQueue<DelayedEntry> queue2 = createBoundedWBQ(counter);
        final WriteBehindQueue<DelayedEntry> queue3 = createBoundedWBQ(counter);
        final WriteBehindQueue<DelayedEntry> queue4 = createBoundedWBQ(counter);
        // put total 1001 items. Max allowed is 1000
        addEnd(10, queue1);
        addEnd(500, queue2);
        addEnd(400, queue3);
        addEnd(91, queue4);
    }

    @Test
    public void testWBQ_counter_is_zero() {
        final AtomicInteger counter = new AtomicInteger(0);
        final WriteBehindQueue<DelayedEntry> queue = createBoundedWBQ(counter);
        addEnd(1000, queue);
        queue.clear();

        assertEquals(0, counter.intValue());
    }

    @Test
    public void testOffer_thenRemove_thenOffer() {
        final WriteBehindQueue<DelayedEntry> queue = createWBQ();
        addEnd(1000, queue);

        queue.clear();

        addEnd(1000, queue);

        assertEquals(1000, queue.size());
    }

    @Test
    public void testCounter_offer_thenRemove() {
        final AtomicInteger counter = new AtomicInteger(0);
        final WriteBehindQueue<DelayedEntry> queue = createBoundedWBQ(counter);
        addEnd(1000, queue);
        queue.drainTo(new ArrayList<DelayedEntry>(1000));

        assertEquals(0, counter.intValue());
    }

    @Test
    public void testClear() {
        final WriteBehindQueue<DelayedEntry> queue = createWBQ();

        queue.clear();

        assertEquals(0, queue.size());
    }

    @Test
    public void testClearFull() {
        final WriteBehindQueue<DelayedEntry> queue = createWBQ();

        addEnd(1000, queue);

        queue.clear();

        assertEquals(0, queue.size());
    }

    @Test
    public void testRemoveAll() {
        final WriteBehindQueue<DelayedEntry> queue = createWBQ();

        addEnd(1000, queue);

        queue.clear();

        assertEquals(0, queue.size());
    }

    @Test
    public void testGet_onCoalescedWBQ_whenCount_smallerThanQueueSize() throws Exception {
        int queueSize = 100;
        int fetchNumberOfEntries = 10;
        WriteBehindQueue<DelayedEntry> wbq = createWBQ();

        testGetWithCount(wbq, queueSize, fetchNumberOfEntries);
    }

    @Test
    public void testGet_onBoundedWBQ_whenCount_smallerThanQueueSize() throws Exception {
        int queueSize = 100;
        int fetchNumberOfEntries = 10;
        WriteBehindQueue<DelayedEntry> wbq = createBoundedWBQ();

        testGetWithCount(wbq, queueSize, fetchNumberOfEntries);
    }

    @Test
    public void testGet_onCoalescedWBQ_whenCount_higherThanQueueSize() throws Exception {
        int queueSize = 100;
        int fetchNumberOfEntries = 10000;
        WriteBehindQueue<DelayedEntry> wbq = createWBQ();

        testGetWithCount(wbq, queueSize, fetchNumberOfEntries);
    }

    @Test
    public void testGet_onBoundedWBQ_whenCount_higherThanQueueSize() throws Exception {
        int queueSize = 100;
        int fetchNumberOfEntries = 10000;
        WriteBehindQueue<DelayedEntry> wbq = createBoundedWBQ();

        testGetWithCount(wbq, queueSize, fetchNumberOfEntries);
    }

    private void testGetWithCount(WriteBehindQueue<DelayedEntry> queue, int queueSize, final int fetchNumberOfEntries) {
        final List<DelayedEntry> delayedEntries = createDelayedEntryList(queueSize);
        for (DelayedEntry entry : delayedEntries) {
            queue.addLast(entry);
        }
        List<DelayedEntry> entries = new ArrayList<DelayedEntry>();
        queue.filter(new IPredicate<DelayedEntry>() {
            int count = 0;

            @Override
            public boolean test(DelayedEntry delayedEntry) {
                return count++ < fetchNumberOfEntries;
            }
        }, entries);

        int expectedFetchedEntryCount = Math.min(queueSize, fetchNumberOfEntries);
        assertEquals(expectedFetchedEntryCount, entries.size());
    }

    private void addEnd(int numberOfEntriesToAdd, WriteBehindQueue<DelayedEntry> queue) {
        List<DelayedEntry> delayedEntries = createDelayedEntryList(numberOfEntriesToAdd);
        for (DelayedEntry entry : delayedEntries) {
            queue.addLast(entry);
        }
    }

    private List<DelayedEntry> createDelayedEntryList(int numberOfEntriesToCreate) {
        final List<DelayedEntry> list = new ArrayList<DelayedEntry>(numberOfEntriesToCreate);
        SerializationService ss1 = new DefaultSerializationServiceBuilder().build();
        final long storeTime = Clock.currentTimeMillis();
        for (int i = 0; i < numberOfEntriesToCreate; i++) {
            final DelayedEntry<Data, Object> e = DelayedEntries.createWithoutValue(ss1.toData(i), storeTime, i);
            list.add(e);
        }
        return list;
    }

    private WriteBehindQueue<DelayedEntry> createBoundedWBQ() {
        final AtomicInteger counter = new AtomicInteger(0);
        return createBoundedWBQ(counter);
    }

    private WriteBehindQueue<DelayedEntry> createBoundedWBQ(AtomicInteger counter) {
        final int maxSizePerNode = 1000;
        return createBoundedWriteBehindQueue(maxSizePerNode, counter);
    }

    private WriteBehindQueue<DelayedEntry> createWBQ() {
        return createDefaultWriteBehindQueue();
    }
}
