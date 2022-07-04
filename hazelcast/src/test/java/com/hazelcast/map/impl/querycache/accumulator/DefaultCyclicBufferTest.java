/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.querycache.accumulator;

import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DefaultCyclicBufferTest {

    @Test(expected = IllegalArgumentException.class)
    public void testBufferCapacity_whenZero() {
        new DefaultCyclicBuffer(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBufferCapacity_whenNegative() {
        new DefaultCyclicBuffer(-1);
    }

    @Test
    public void testBufferSize_whenEmpty() {
        int maxCapacity = nextPowerOfTwo(10);
        CyclicBuffer buffer = new DefaultCyclicBuffer(maxCapacity);

        assertEquals("item count should be = " + 0, 0, buffer.size());
    }

    @Test
    public void testBufferSize_whenAddedOneItem() {
        int maxCapacity = nextPowerOfTwo(10);
        int itemCount = 1;
        CyclicBuffer<TestSequenced> buffer = new DefaultCyclicBuffer<TestSequenced>(maxCapacity);

        for (int i = 1; i <= itemCount; i++) {
            buffer.add(new TestSequenced(i));
        }

        assertEquals("item count should be = " + itemCount, itemCount, buffer.size());
    }

    @Test
    public void testBufferSize_whenFilledLessThanCapacity() {
        int maxCapacity = nextPowerOfTwo(10);
        int itemCount = 4;
        CyclicBuffer<TestSequenced> buffer = new DefaultCyclicBuffer<TestSequenced>(maxCapacity);

        for (int i = 1; i <= itemCount; i++) {
            buffer.add(new TestSequenced(i));
        }

        assertEquals("item count should be = " + itemCount, itemCount, buffer.size());
    }

    @Test
    public void testBufferSize_whenFilledMoreThanCapacity() {
        int maxCapacity = nextPowerOfTwo(4);
        int itemCount = 40;
        CyclicBuffer<TestSequenced> buffer = new DefaultCyclicBuffer<TestSequenced>(maxCapacity);

        for (int i = 1; i <= itemCount; i++) {
            buffer.add(new TestSequenced(i));
        }

        assertEquals("item count should be = " + maxCapacity, maxCapacity, buffer.size());
    }


    @Test
    public void testBufferRead_withSequence() {
        int maxCapacity = nextPowerOfTwo(10);
        int itemCount = 4;
        CyclicBuffer<TestSequenced> buffer = new DefaultCyclicBuffer<TestSequenced>(maxCapacity);

        for (int i = 1; i <= itemCount; i++) {
            buffer.add(new TestSequenced(i));
        }

        long readFromSequence = 1L;
        do {
            Sequenced sequenced = buffer.get(readFromSequence);
            if (sequenced == null) {
                break;
            }
            if (readFromSequence + 1 > itemCount) {
                break;
            }
            readFromSequence++;
        } while (true);

        assertEquals("read count should be = " + readFromSequence, readFromSequence, itemCount);
    }

    @Test
    public void testSetHead_returnsTrue_whenSuppliedSequenceInBuffer() {
        int maxCapacity = nextPowerOfTwo(16);
        int itemCount = 40;
        CyclicBuffer<TestSequenced> buffer = new DefaultCyclicBuffer<TestSequenced>(maxCapacity);

        for (int i = 1; i <= itemCount; i++) {
            buffer.add(new TestSequenced(i));
        }

        long suppliedSequence = 37;

        boolean setHead = buffer.setHead(suppliedSequence);

        assertTrue("setHead call should be successful", setHead);
    }

    @Test
    public void testSetHead_returnsFalse_whenSuppliedSequenceIsNotInBuffer() {
        int maxCapacity = nextPowerOfTwo(16);
        int itemCount = 40;
        CyclicBuffer<TestSequenced> buffer = new DefaultCyclicBuffer<TestSequenced>(maxCapacity);

        for (int i = 1; i <= itemCount; i++) {
            buffer.add(new TestSequenced(i));
        }

        long suppliedSequence = 3;

        boolean setHead = buffer.setHead(suppliedSequence);

        assertFalse("setHead call should fail", setHead);
    }

    @Test
    public void testSetHead_changesBufferSize_whenSucceeded() {
        int itemCount = 40;
        int maxCapacity = nextPowerOfTwo(16);
        CyclicBuffer<TestSequenced> buffer = new DefaultCyclicBuffer<TestSequenced>(maxCapacity);

        for (int i = 1; i <= itemCount; i++) {
            buffer.add(new TestSequenced(i));
        }
        long newSequence = 37;
        buffer.setHead(newSequence);

        assertEquals("buffer size should be affected from setting new sequence", itemCount - newSequence + 1, buffer.size());
    }

    @Test
    public void testSetHead_doesNotChangeBufferSize_whenFailed() {
        int maxCapacity = nextPowerOfTwo(16);
        int itemCount = 40;
        CyclicBuffer<TestSequenced> buffer = new DefaultCyclicBuffer<TestSequenced>(maxCapacity);

        for (int i = 1; i <= itemCount; i++) {
            buffer.add(new TestSequenced(i));
        }
        long newSequence = 3;
        buffer.setHead(newSequence);

        assertEquals("buffer size should not be affected", maxCapacity, buffer.size());
    }

    @Test
    public void test_size() throws Exception {
        int maxCapacity = nextPowerOfTwo(10);
        CyclicBuffer<TestSequenced> buffer = new DefaultCyclicBuffer<TestSequenced>(maxCapacity);

        for (int i = 1; i < 2; i++) {
            buffer.add(new TestSequenced(i));
        }

        assertEquals(1, buffer.size());
    }

    private static class TestSequenced implements Sequenced {

        private long sequence;

        TestSequenced(long seq) {
            this.sequence = seq;
        }

        @Override
        public long getSequence() {
            return sequence;
        }

        @Override
        public int getPartitionId() {
            return 0;
        }

        @Override
        public void setSequence(long sequence) {
            this.sequence = sequence;
        }

        @Override
        public String toString() {
            return "TestSequenced{"
                    + "sequence=" + sequence
                    + '}';
        }
    }
}
