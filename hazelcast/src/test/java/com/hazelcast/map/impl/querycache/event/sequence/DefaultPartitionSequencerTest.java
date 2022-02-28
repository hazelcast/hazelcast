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

package com.hazelcast.map.impl.querycache.event.sequence;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DefaultPartitionSequencerTest {

    private DefaultPartitionSequencer sequencer = new DefaultPartitionSequencer();

    @Test
    public void testNextSequence() {
        assertEquals(1, sequencer.nextSequence());
        assertEquals(2, sequencer.nextSequence());
    }

    @Test
    public void testSetSequence() {
        sequencer.setSequence(23);

        assertEquals(23, sequencer.getSequence());
    }

    @Test
    public void testCompareAndSetSequence() {
        sequencer.compareAndSetSequence(23, 42);
        assertEquals(0, sequencer.getSequence());

        sequencer.compareAndSetSequence(0, 42);
        assertEquals(42, sequencer.getSequence());
    }

    @Test
    public void testGetSequence() {
        assertEquals(0, sequencer.getSequence());
    }

    @Test
    public void testReset() {
        sequencer.setSequence(42);
        sequencer.reset();

        assertEquals(0, sequencer.getSequence());
    }
}
