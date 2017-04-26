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

import com.hazelcast.jet.Accumulators.MutableLong;
import com.hazelcast.jet.Processor.Context;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class GroupByFramePTest extends StreamingTestSupport {

    private static final long KEY = 77L;
    private GroupByFrameP<Entry<Long, Long>, Long, ?> processor;

    @Before
    public void before() {
        processor = WindowingProcessors.groupByFrame(
                (Entry<Long, Long> x) -> KEY,
                Entry::getKey,
                new WindowDefinition(4, 0, 4),
                WindowOperations.summingToLong((Entry<Long, Long> e) -> e.getValue())
        ).get();
        processor.init(outbox, mock(Context.class));
    }

    @After
    public void after() {
        assertTrue("map not empty after emitting everything", processor.seqToKeyToFrame.isEmpty());
    }

    @Test
    public void smokeTest() {
        // Given
        ArrayDequeInbox inbox = new ArrayDequeInbox();
        inbox.addAll(asList(
                entry(0L, 1L), // to frame 4
                entry(1L, 1L), // to frame 4
                punc(3), // does not close anything
                entry(2L, 1L), // to frame 4, still accepted, even though it's late after punc(3)
                punc(4), // closes frame 4
                entry(4L, 1L), // to frame 8
                entry(5L, 1L), // to frame 8
                entry(8L, 1L), // to frame 12
                punc(6), // no effect
                punc(7), // no effect
                entry(8L, 1L), // to frame 12
                punc(8), // closes frame 8
                entry(8L, 1L), // to frame 12
                punc(21) // closes everything
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                punc(3),
                frame(4, 3),
                punc(4),
                punc(6),
                punc(7),
                frame(8, 2),
                punc(8),
                frame(12, 3),
                punc(21)
        ));
    }

    @Test
    public void when_noEvents_then_punctsEmitted() {
        // Given
        ArrayDequeInbox inbox = new ArrayDequeInbox();
        List<Punctuation> somePuncs = asList(
                punc(2),
                punc(3),
                punc(4),
                punc(5),
                punc(6),
                punc(8),
                punc(20)
        );
        inbox.addAll(somePuncs);

        // When
        processor.process(0, inbox);

        // Then
        assertOutbox(somePuncs);
    }

    private static Frame<Long, MutableLong> frame(long seq, long value) {
        return new Frame<>(seq, KEY, new MutableLong(value));
    }
}
