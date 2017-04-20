/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.windowing;

import com.hazelcast.jet.Processor.Context;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.impl.util.ProgressTracker;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class InsertPunctuationPTest {

    private static final long LAG = 3;

    private MockClock clock;
    private InsertPunctuationP<Item> p;
    private ArrayDequeOutbox outbox;
    private List<String> resultToCheck = new ArrayList<>();

    public void setUp(int outboxCapacity) {
        clock = new MockClock(100);
        p = new InsertPunctuationP<>(Item::getTime, PunctuationPolicies.cappingEventSeqLag(LAG));

        outbox = new ArrayDequeOutbox(new int[]{outboxCapacity}, new ProgressTracker());
        Context context = mock(Context.class);

        p.init(outbox, context);
    }

    @Test
    public void smokeTest_oneItemOutbox() throws Exception {
        smokeTest(1);
    }

    @Test
    public void smoteTest_outboxLargeEnough() throws Exception {
        smokeTest(1024);
    }

    public void smokeTest(int outboxCapacity) throws Exception {
        setUp(outboxCapacity);

        // this is to make the capacity-one outbox initially full
        assertTrue(outbox.offer("initialItem"));

        String[] expected = {
                "-- at 100",
                "initialItem",
                "Punctuation{seq=7}",
                "Item{time=10}",
                "Item{time=8}",
                "-- at 101",
                "Punctuation{seq=8}",
                "Item{time=11}",
                "Item{time=9}",
                "-- at 102",
                "Punctuation{seq=9}",
                "Item{time=12}",
                "Item{time=10}",
                "-- at 103",
                "Punctuation{seq=10}",
                "Item{time=13}",
                "Item{time=11}",
                "-- at 104",
                "-- at 105",
                "-- at 106",
                "-- at 107",
                "-- at 108",
                "-- at 109",
                "-- at 110",
                "Punctuation{seq=17}",
                "Item{time=20}",
                "Item{time=18}",
                "-- at 111",
                "Punctuation{seq=18}",
                "Item{time=21}",
                "Item{time=19}",
                "-- at 112",
                "-- at 113",
                "-- at 114",
                "-- at 115",
                "-- at 116",
                "-- at 117",
                "-- at 118",
                "-- at 119",
        };

        for (int eventTime = 10; eventTime < 30; eventTime++) {
            resultToCheck.add("-- at " + clock.time());
            if (eventTime < 14 || eventTime >= 20 && eventTime <= 21) {
                Item item = new Item(eventTime);
                Item oldItem = new Item(eventTime - 2);
                tryProcessAndDrain(item);
                tryProcessAndDrain(oldItem);
            }

            p.tryProcess();
            drainOutbox(resultToCheck);

            clock.advance();
        }

        assertEquals(toString(Arrays.asList(expected)), toString(resultToCheck));
    }

    private void tryProcessAndDrain(Item item) throws Exception {
        while (!p.tryProcess(0, item)) {
            drainOutbox(resultToCheck);
        }
    }

    private void drainOutbox(List<String> actual) {
        for (Object o; (o = outbox.queueWithOrdinal(0).poll()) != null; )
            actual.add(o.toString());
    }

    private static String toString(List<String> actual) {
        return actual.stream().collect(Collectors.joining("\n"));
    }

    private static class Item {
        final long time;

        Item(long time) {
            this.time = time;
        }

        public long getTime() {
            return time;
        }

        @Override
        public String toString() {
            return "Item{time=" + time + '}';
        }

        @Override
        public boolean equals(Object o) {
            return this == o || o instanceof Item && this.time == ((Item) o).time;
        }

        @Override
        public int hashCode() {
            return (int) (time ^ (time >>> 32));
        }
    }

    private static class MockClock {
        long time;

        MockClock(long time) {
            this.time = time;
        }

        long time() {
            return time;
        }

        void advance() {
            time++;
        }
    }
}
