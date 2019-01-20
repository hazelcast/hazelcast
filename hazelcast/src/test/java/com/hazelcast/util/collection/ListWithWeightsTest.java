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

package com.hazelcast.util.collection;

import com.hazelcast.util.collection.WeightedEvictableList.WeightedItem;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ListWithWeightsTest {

    @Test
    public void testGetVote() {
        WeightedEvictableList<String> list = new WeightedEvictableList<String>(3, 3);
        list.add("a");
        list.add("b");
        list.add("c");

        List<WeightedItem<String>> snapshot = list.getSnapshot();
        assertSnapshot(snapshot, "a", "b", "c");

        list.voteFor(snapshot.get(2));
        list.voteFor(snapshot.get(1));
        list.voteFor(snapshot.get(2));
        // votes so far a: 1, b: 2, c: 3; 3 iterations are here, now list will organize itself
        snapshot = list.getSnapshot();
        assertSnapshot(snapshot, "c", "b", "a");

        // votes are 0, 0, 0
        list.voteFor(snapshot.get(1));
        list.voteFor(snapshot.get(0));
        // votes a:0, b:1, c:1
        list.add("d");
        // "a" should go because it has 0 votes

        snapshot = list.getSnapshot();
        assertSnapshot(snapshot, "c", "d");

        list.add("x");
        snapshot = list.getSnapshot();
        assertSnapshot(snapshot, "c", "d", "x");
        list.voteFor(snapshot.get(2));
        list.voteFor(snapshot.get(2));
        list.voteFor(snapshot.get(2));
        snapshot = list.getSnapshot();
        assertSnapshot(snapshot, "x", "c", "d");
    }

    @Test
    public void testMultithreadStress() throws InterruptedException {
        final int threadCount = 10;
        final int keySpace = 50;
        final int listMaxSize = 30;
        final int listMaxVotesBeforeReorganization = 8000;
        Thread[] threads = new Thread[threadCount];

        WeightedEvictableList<Integer> list = new WeightedEvictableList<Integer>(listMaxSize, listMaxVotesBeforeReorganization);
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        AtomicBoolean stop = new AtomicBoolean();

        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(new OpRunner(list, stop, keySpace));
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].start();
        }

        Thread.sleep(25000);
        stop.set(true);
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }

        assertListWithWeightsIntegrity(list, listMaxSize);
    }

    private void assertListWithWeightsIntegrity(WeightedEvictableList list, int maxSize) {
        Set uniqueKeys = new HashSet();
        list.organize(null);
        List<WeightedItem> snapshot = list.getSnapshot();
        assertTrue(maxSize >= snapshot.size());
        assertTrue(maxSize / 2 <= snapshot.size());
        int previousItemWeight = Integer.MAX_VALUE;
        for (WeightedItem item: snapshot) {
            assertTrue(previousItemWeight >= item.getWeightUnsafe());
            assertFalse("Item " + item.getItem() + " is not unique in snapshot", uniqueKeys.contains(item.getItem()));
            uniqueKeys.add(item.getItem());
            previousItemWeight = item.getWeightUnsafe();
        }
    }

    private static class OpRunner implements Runnable {

        private final WeightedEvictableList<Integer> list;
        private final AtomicBoolean stop;
        private final Random random = new Random();
        private final int keySpace;

        public OpRunner(WeightedEvictableList<Integer> list, AtomicBoolean stop, int keySpace) {
            this.list = list;
            this.stop = stop;
            this.keySpace = keySpace;
        }

        @Override
        public void run() {
            while (!stop.get()) {
                int op = random.nextInt(100);
                if (op < 70) {
                    List<WeightedItem<Integer>> snapshot = list.getSnapshot();
                    list.voteFor(snapshot.get(random.nextInt(snapshot.size())));
                } else {
                    list.add(random.nextInt(keySpace));
                }
            }
        }
    }

    private <T> void assertSnapshot(List<WeightedItem<T>> snapshot, T... values) {
        for (int i = 0; i < values.length; i++) {
            assertEquals("Item " + i + " at snapshot is not correct", values[i], snapshot.get(i).getItem());
        }
    }
}
