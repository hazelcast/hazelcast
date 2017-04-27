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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map.Entry;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class TumblingWindowPTest extends StreamingTestSupport {

    private static final long KEY = 77L;
    private TumblingWindowP<Object, ?, Long> processor;

    @Before
    public void before() {
        WindowOperation<Entry<Long, Long>, ?, Long> operation = WindowOperations.summingToLong(Entry::getValue);

        processor = new TumblingWindowP<>(WindowDefinition.tumblingWindowDef(1), operation);
        processor.init(outbox, mock(Context.class));
    }

    @After
    public void after() {
        assertTrue("map not empty after emitting everything", processor.seqToKeyToFrame.isEmpty());
    }

    @Test
    public void smokeTest() {
        // Given
        inbox.addAll(asList(
                frame(2, 1),
                punc(2),
                frame(3, 1),
                frame(3, 2),
                punc(3),
                frame(5, 1),
                frame(4, 1),
                punc(5)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                outboxFrame(2, 1),
                punc(2),
                outboxFrame(3, 3),
                punc(3),
                outboxFrame(4, 1),
                outboxFrame(5, 1),
                punc(5)
        ));
    }

    private static Frame<Long, MutableLong> frame(long seq, long value) {
        return new Frame<>(seq, KEY, new MutableLong(value));
    }

    private static Frame<Long, ?> outboxFrame(long seq, long value) {
        return new Frame<>(seq, KEY, value);
    }
}
