/*
 * Original work Copyright 2016 Real Logic Ltd.
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.collection;

import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConcurrentArrayQueueTest {
    @Parameters(name = "manyToOne == {0}")
    public static Collection<Object[]> params() {
        return asList(new Object[][] { {false}, {true} });
    }

    @Parameter public boolean manyToOne;

    private QueuedPipe<Integer> q;

    @Before public void setup() {
        this.q = manyToOne ? new ManyToOneConcurrentArrayQueue<Integer>(2)
                           : new OneToOneConcurrentArrayQueue<Integer>(2);
    }

    @Test public void when_offer_then_pollInOrder() {
        q.offer(1);
        q.offer(2);
        assertEquals(1, (Object) q.poll());
        assertEquals(2, (Object) q.poll());
    }

    @Test public void when_offerTooMany_then_rejectWithFalse() {
        assertTrue(q.offer(1));
        assertTrue(q.offer(2));
        assertFalse(q.offer(3));
    }

    @Test public void when_pollTooMany_then_getNull() {
        q.offer(1);
        assertNotNull(q.poll());
        assertNull(q.poll());
    }

    @Test public void when_offer_then_drainToList() {
        q.offer(1);
        q.offer(2);
        final List<Integer> drained = new ArrayList<Integer>();
        q.drainTo(drained, 2);
        assertEquals(drained, asList(1, 2));
    }

    @Test public void when_add_then_removeInOrder() {
        q.add(1);
        q.add(2);
        assertEquals(1, (Object) q.remove());
        assertEquals(2, (Object) q.remove());
    }

    @Test(expected = IllegalStateException.class)
    public void when_addTooMany_then_exception() {
        q.add(1);
        q.add(2);
        q.add(3);
    }

    @Test(expected = NoSuchElementException.class)
    public void when_removeTooMany_then_exception() {
        q.add(1);
        q.remove();
        q.remove();
    }

    @Test public void when_addAndRemove_then_addedCountTracksTotalAdded() {
        q.add(4);
        q.remove();
        q.add(3);
        assertEquals(2, q.addedCount());
    }

    @Test public void when_instantiateWithCapacityX_then_capacityReportsX() {
        assertEquals(2, q.capacity());
    }

    @Test public void when_clear_then_pollNull() {
        q.add(1);
        q.clear();
        assertNull(q.poll());
    }

    @Test public void when_add_then_contains() {
        q.add(1);
        assertTrue(q.contains(1));
    }

    @Test public void when_addSome_thenContainsAll() {
        q.add(1);
        q.add(2);
        assertTrue(q.containsAll(asList(1, 2)));
    }

    @Test public void when_add_then_elementGetsIt() {
        q.add(1);
        assertEquals(1, (Object) q.element());
    }

    @Test(expected = NoSuchElementException.class)
    public void when_addAndRemove_then_elementThrows() {
        q.add(1);
        q.remove();
        q.element();
    }

    @Test public void when_empty_then_isEmptyGetsTrue() {
        assertTrue(q.isEmpty());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void when_iterator_thenUnsupportedException() {
        q.iterator();
    }

    @Test public void when_add_then_peek() {
        q.add(1);
        assertEquals(1, (Object) q.peek());
    }

    @Test public void when_add_then_remaningCapacityReduced() {
        q.add(1);
        assertEquals(1, q.remainingCapacity());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void when_removeSpecificObject_thenUnsupportedException() {
        q.add(1);
        q.remove(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void when_removeAll_thenUnsupportedException() {
        q.add(1);
        q.removeAll(singletonList(1));
    }

    @Test public void when_addAndRemove_thenRemovedCountTracksTotalRemoved() {
        q.add(1);
        q.remove();
        assertEquals(1, q.removedCount());
    }

    @Test public void when_addAndRemove_thenSizeZero() {
        q.add(1);
        q.remove();
        assertEquals(0, q.size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void when_toArray_thenUnsupportedException() {
        q.toArray();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void when_toArrayWithArg_thenUnsupportedException() {
        q.toArray(null);
    }
}
