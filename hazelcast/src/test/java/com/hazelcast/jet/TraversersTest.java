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

package com.hazelcast.jet;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Traversers.traverseEnumeration;
import static com.hazelcast.jet.Traversers.traverseItems;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseIterator;
import static com.hazelcast.jet.Traversers.traverseSpliterator;
import static com.hazelcast.jet.Traversers.traverseStream;
import static java.util.Arrays.asList;
import static java.util.stream.Stream.of;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TraversersTest {

    @Test
    public void when_traverserOverArgs_then_seeAllItems() {
        validateTraversal(traverseItems(1, 2));
    }

    @Test
    public void when_traverseIterator_then_seeAllItems() {
        validateTraversal(traverseIterator(Stream.of(1, 2).iterator()));
    }

    @Test
    public void when_traverseSpliterator_then_seeAllItems() {
        validateTraversal(traverseSpliterator(Stream.of(1, 2).spliterator()));
    }

    @Test
    public void when_traverseEnumeration_then_seeAllItems() {
        validateTraversal(traverseEnumeration(new Vector<>(asList(1, 2)).elements()));
    }

    @Test
    public void when_traverseStream_then_seeAllItems() {
        validateTraversal(traverseStream(of(1, 2)));
    }

    @Test
    public void when_traverseIterable_then_seeAllItems() {
        validateTraversal(traverseIterable(asList(1, 2)));
    }

    @Test
    public void when_traverseArray_then_seeAllItems() {
        validateTraversal(traverseArray(new Integer[] {1, 2}));
    }

    @Test
    public void when_traverserOverArgsWithNull_then_skipNulls() {
        Traverser<Integer> trav = traverseItems(1, null, 2);
        assertEquals(1, (int) trav.next());
        assertEquals(2, (int) trav.next());
        assertNull(trav.next());
    }

    @Test
    public void when_traverseArrayWithNull_then_skipNulls() {
        Traverser<Integer> trav = traverseArray(new Integer[] {1, null, 2});
        assertEquals(1, (int) trav.next());
        assertEquals(2, (int) trav.next());
        assertNull(trav.next());
    }

    @Test(expected = NullPointerException.class)
    public void when_traverseIteratorWithNull_then_failure() {
        Traverser<Integer> trav = traverseIterator(asList(1, null).iterator());
        trav.next();
        trav.next();
    }

    @Test
    public void when_traverseIteratorIgnoringNulls_then_filteredOut() {
        Traverser<Integer> trav = traverseIterator(asList(null, 1, null, 2, null).iterator(), true);
        assertEquals(1, (int) trav.next());
        assertEquals(2, (int) trav.next());
        assertNull(trav.next());
    }

    @Test(expected = NullPointerException.class)
    public void when_traverseSpliteratorWithNull_then_failure() {
        Traverser<Integer> trav = traverseSpliterator(Stream.of(1, null).spliterator());
        trav.next();
        trav.next();
    }

    @Test(expected = NullPointerException.class)
    public void when_traverseEnumerationWithNull_then_failure() {
        Traverser<Integer> trav = traverseEnumeration(new Vector<>(asList(1, null)).elements());
        trav.next();
        trav.next();
    }

    @Test(expected = NullPointerException.class)
    public void when_traverseIterableWithNull_then_failure() {
        Traverser<Integer> trav = traverseIterable(asList(1, null));
        trav.next();
        trav.next();
    }

    @Test
    public void when_traverseStreamConsumed_then_streamClosed() {
        // Given
        boolean[] closed = {false};
        Traverser<Integer> trav = traverseStream(of(1).onClose(() -> closed[0] = true));
        trav.next();
        assertFalse(closed[0]);

        // When
        trav.next();

        // Then
        assertTrue(closed[0]);
    }

    @Test
    public void when_lazyLeftAlone_then_supplierNotCalled() {
        Traversers.lazy(() -> {
            fail();
            return null;
        });
    }

    @Test
    public void lazyTraversalTransparent() {
        validateTraversal(Traversers.lazy(() -> traverseStream(of(1, 2))));
    }

    private static void validateTraversal(Traverser<?> trav) {
        // When
        final Object t1 = trav.next();
        final Object t2 = trav.next();
        final Object t3 = trav.next();

        // Then
        assertEquals(1, t1);
        assertEquals(2, t2);
        assertNull(t3);
    }

    @Test
    public void peek() {
        List<Integer> list = new ArrayList<>();
        Traverser<Integer> t = traverseItems(1, 2, 3).peek(list::add);

        assertEquals(Integer.valueOf(1), t.next());
        assertEquals(Integer.valueOf(2), t.next());
        assertEquals(Integer.valueOf(3), t.next());
        assertNull(t.next());

        assertEquals(Arrays.asList(1, 2, 3), list);
    }
}
