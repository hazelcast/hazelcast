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

package com.hazelcast.jet.impl.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class ArrayDequeInboxTest {

    private static final Integer ITEM = 1;

    private ArrayDequeInbox inbox = new ArrayDequeInbox(new ProgressTracker());

    @Before
    public void before() {
        inbox.queue().add(ITEM);
    }

    @Test
    public void when_pollNonEmpty_then_getItem() {
        assertEquals(ITEM, inbox.poll());
    }

    @Test
    public void when_pollEmpty_then_getNull() {
        inbox.queue().clear();
        assertNull(inbox.poll());
    }

    @Test
    public void when_removeNonEmpty_then_removeItem() {
        inbox.remove();
        assertTrue(inbox.isEmpty());
    }

    @Test(expected = NoSuchElementException.class)
    public void when_removeEmpty_then_getException() {
        inbox.queue().clear();
        inbox.remove();
    }

    @Test
    public void when_drainToCollection_then_allDrained() {
        ArrayList<Object> sink = new ArrayList<>();
        inbox.drainTo(sink);
        assertEquals(singletonList(ITEM), sink);
    }

    @Test
    public void when_drainToConsumer_then_allDrained() {
        ArrayList<Object> sink = new ArrayList<>();
        inbox.drain(sink::add);
        assertEquals(singletonList(ITEM), sink);
    }
}
