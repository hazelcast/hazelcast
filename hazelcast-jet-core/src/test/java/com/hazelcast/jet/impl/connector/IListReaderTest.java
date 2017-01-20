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

import com.hazelcast.jet.Processor;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.Queue;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
public class IListReaderTest {

    @Test
    public void when_sizeLessThanFetchSize_then_readAll() {
        testReader(13);
    }

    @Test
    public void when_sizeMoreThanFetchSize_then_readAll() {
        testReader(3);
    }

    private static void testReader(int fetchSize) {
        final ArrayDequeOutbox outbox = new ArrayDequeOutbox(1, new int[]{2});
        final Queue<Object> bucket = outbox.queueWithOrdinal(0);
        final IListReader r = new IListReader(asList(1, 2, 3, 4), fetchSize);
        r.init(outbox, Mockito.mock(Processor.Context.class));

        // When
        assertFalse(r.complete());

        // Then
        assertEquals(1, bucket.poll());
        assertEquals(2, bucket.poll());
        assertNull(bucket.poll());

        // When
        assertTrue(r.complete());

        // Then
        assertEquals(3, bucket.poll());
        assertEquals(4, bucket.poll());
        assertNull(bucket.poll());
    }

}
