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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.Inbox;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
public class IListWriterTest {

    @Test
    public void when_processItems_then_addedToList() {
        final List<Object> sink = new ArrayList<>();
        final IListWriter w = new IListWriter(sink);
        final MockInbox inbox = new MockInbox();
        final List<Integer> input = asList(1, 2, 3, 4);
        inbox.addAll(input);

        // When
        w.process(0, inbox);

        // Then
        assertEquals(input, sink);
    }

    private static class MockInbox extends ArrayDeque<Object> implements Inbox {
    }
}
