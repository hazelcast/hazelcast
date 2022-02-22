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

import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.Traversers.traverseItems;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("ResultOfMethodCallIgnored")
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TraverserTest {

    private AppendableTraverser<Integer> rootTraverser = new AppendableTraverser<>(3);

    private AppendableTraverser<Integer> resetRootTraverser() {
        while (rootTraverser.next() != null) {
        }
        rootTraverser.append(0);
        rootTraverser.append(1);
        rootTraverser.append(2);
        return rootTraverser;
    }

    @Test
    public void smokeTest_map() {
        check(resetRootTraverser().map(identity()),
                asList(0, 1, 2), singletonList(3));
        check(resetRootTraverser().map(i -> i + 1),
                asList(1, 2, 3), singletonList(4));
        check(resetRootTraverser().map(i -> i % 2 != 0 ? null : i),
                asList(0, 2), emptyList());
        check(resetRootTraverser().map(i -> null),
                emptyList(), emptyList());
        check(resetRootTraverser().map(i -> i == 0 ? null : i),
                asList(1, 2), singletonList(3));
        check(resetRootTraverser().map(i -> i != 0 ? null : i),
                singletonList(0), emptyList());
    }

    @Test
    public void smokeTest_filter() {
        check(resetRootTraverser().filter(i -> true),
                asList(0, 1, 2), singletonList(3));
        check(resetRootTraverser().filter(i -> false),
                emptyList(), emptyList());
        check(resetRootTraverser().filter(i -> i != 1),
                asList(0, 2), singletonList(3));
        check(resetRootTraverser().filter(i -> i == 1),
                singletonList(1), emptyList());
        check(resetRootTraverser().filter(i -> i != 0),
                asList(1, 2), singletonList(3));
        check(resetRootTraverser().filter(i -> i != 2),
                asList(0, 1), singletonList(3));
        check(resetRootTraverser().filter(i -> i != 3),
                asList(0, 1, 2), emptyList());
    }

    @Test
    public void smokeTest_takeWhile() {
        check(resetRootTraverser().takeWhile(i -> false),
                emptyList(), emptyList());
        check(resetRootTraverser().takeWhile(i -> true),
                asList(0, 1, 2), singletonList(3));
        check(resetRootTraverser().takeWhile(i -> i < 3),
                asList(0, 1, 2), emptyList());
        check(resetRootTraverser().takeWhile(i -> i < 2),
                asList(0, 1), emptyList());
        check(resetRootTraverser().takeWhile(i -> i < 1),
                singletonList(0), emptyList());
        check(resetRootTraverser().takeWhile(i -> i < 0),
                emptyList(), emptyList());
    }

    @Test
    public void smokeTest_dropWhile() {
        check(resetRootTraverser().dropWhile(i -> true),
                emptyList(), emptyList());
        check(resetRootTraverser().dropWhile(i -> false),
                asList(0, 1, 2), singletonList(3));
        check(resetRootTraverser().dropWhile(i -> i < 3),
                emptyList(), singletonList(3));
        check(resetRootTraverser().dropWhile(i -> i < 2),
                singletonList(2), singletonList(3));
        check(resetRootTraverser().dropWhile(i -> i < 1),
                asList(1, 2), singletonList(3));
        check(resetRootTraverser().dropWhile(i -> i < 0),
                asList(0, 1, 2), singletonList(3));
    }

    @Test
    public void smokeTest_append() {
        check(traverseItems(0, 1, 2).append(5),
                asList(0, 1, 2, 5), emptyList());
        check(traverseItems(0, 1, 2).append(5).append(6),
                asList(0, 1, 2, 5, 6), emptyList());
        check(Traversers.empty().append(0),
                singletonList(0), emptyList());
    }

    @Test
    public void smokeTest_prepend() {
        check(resetRootTraverser().prepend(5),
                asList(5, 0, 1, 2), singletonList(3));
        check(resetRootTraverser().prepend(5).prepend(6),
                asList(6, 5, 0, 1, 2), singletonList(3));
        check(Traversers.empty().prepend(0),
                singletonList(0), emptyList());
    }

    @Test
    public void smokeTest_flatMap() {
        check(resetRootTraverser().flatMap(i -> traverseItems(0, 1, 2)),
                asList(0, 1, 2, 0, 1, 2, 0, 1, 2), asList(0, 1, 2));
        check(resetRootTraverser().flatMap(i -> Traversers.empty()),
                emptyList(), emptyList());
        check(resetRootTraverser().flatMap(i -> traverseItems(0, 1, 2).takeWhile(j -> j < i)),
                asList(0, 0, 1), asList(0, 1, 2)); // 0 -> {}, 1 -> {0}, 2 -> {0, 1} ==> {0, 0, 1}
    }

    @Test
    public void flatMap_continueAfterNull() {
        AppendableTraverser<Integer> rootTrav = new AppendableTraverser<>(1);
        rootTrav.append(0);
        Traverser<Integer> mappedTrav = rootTrav.flatMap(Traversers::traverseItems);
        assertEquals(Integer.valueOf(0), mappedTrav.next());
        assertNull(mappedTrav.next());
        // the root traverser was already drained (returned null), now add more items to it and check that they
        // are flat-mapped
        rootTrav.append(1);
        assertEquals(Integer.valueOf(1), mappedTrav.next());
    }

    @Test
    public void when_flatMapAdded_then_upstreamItemNotTaken() {
        Traverser<Object> trav = () -> {
            // Then
            throw new AssertionError("item must not be taken");
        };

        // When
        // the item from the upstream must be taken when `next()` is called for
        // the first time. Traverser pipelines are often constructed in initialization
        // stages and iterated later. If we take the item now, some parts might
        // not be initialized.
        Traverser<Object> flatMapped = trav.flatMap(t -> Traversers.empty());

        try {
            flatMapped.next();
            fail("should have failed");
        } catch (AssertionError e) {
            assertEquals("item must not be taken", e.getMessage());
        }
    }

    @Test
    public void smokeTest_peek() {
        List<Integer> peekList = new ArrayList<>();
        check(resetRootTraverser().peek(peekList::add),
                asList(0, 1, 2), singletonList(3));
        assertEquals(asList(0, 1, 2, 3), peekList);
        peekList.clear();

        // check that it is not peeking item appended after the peek
        check(resetRootTraverser().peek(peekList::add).append(4),
                asList(0, 1, 2, 4), singletonList(3));
        assertEquals(asList(0, 1, 2, 3), peekList);
        peekList.clear();

        // check that it is not peeking the item prepended after the peek
        check(resetRootTraverser().peek(peekList::add).prepend(4),
                asList(4, 0, 1, 2), singletonList(3));
        assertEquals(asList(0, 1, 2, 3), peekList);
        peekList.clear();

        // check that it is peeking the items filtered-out after peek()
        check(resetRootTraverser().peek(peekList::add).filter(i -> i == 0),
                singletonList(0), emptyList());
        assertEquals(asList(0, 1, 2, 3), peekList);
        peekList.clear();
    }

    @Test
    public void smokeTest_onFirstNull() {
        boolean[] actionCalled = {false};
        Traverser<Integer> t = resetRootTraverser()
                .onFirstNull(() -> {
                    assert !actionCalled[0] : "action already called";
                    actionCalled[0] = true;
                });

        assertEquals(0, (int) t.next());
        assertFalse(actionCalled[0]);
        assertEquals(1, (int) t.next());
        assertFalse(actionCalled[0]);
        assertEquals(2, (int) t.next());
        assertFalse(actionCalled[0]);
        assertNull(t.next());
        assertTrue(actionCalled[0]);
        assertNull(t.next()); // this will fail inside the action if called duplicately
        rootTraverser.append(3);
        assertEquals(3, (int) t.next());
        assertNull(t.next());
    }

    @Test
    public void when_allFilteredOut_then_onFirstNullCalledImmediately() {
        boolean[] actionCalled = {false};
        Traverser<Integer> t = resetRootTraverser()
                .filter(i -> false)
                .onFirstNull(() -> {
                    assert !actionCalled[0] : "action already called";
                    actionCalled[0] = true;
                });

        assertFalse(actionCalled[0]);
        assertNull(t.next());
        assertTrue(actionCalled[0]);
    }

    @Test
    public void when_allFilteredOutUsingMap_then_onFirstNullCalledImmediately() {
        boolean[] actionCalled = {false};
        Traverser<Integer> t = resetRootTraverser()
                .map(i -> (Integer) null)
                .onFirstNull(() -> {
                    assert !actionCalled[0] : "action already called";
                    actionCalled[0] = true;
                });

        assertFalse(actionCalled[0]);
        assertNull(t.next());
        assertTrue(actionCalled[0]);
    }

    private <T> void check(Traverser<T> t, List<T> expected, List<T> expectedAfterAdd) {
        List<T> list = new ArrayList<>();
        for (T item; (item = t.next()) != null; ) {
            list.add(item);
        }
        assertEquals(expected, list);
        // after first null there should be only nulls
        assertNull(t.next());
        assertNull(t.next());

        // if `expectedAfterAdd` is not null, append one more item to rootTraverser and
        // assert the traverser output again
        if (expectedAfterAdd != null) {
            // add one more item to the rootTraverser
            rootTraverser.append(3);
            list.clear();
            for (T item; (item = t.next()) != null; ) {
                list.add(item);
            }
            assertEquals(expectedAfterAdd, list);
            assertNull(t.next());
            assertNull(t.next());
        }
    }
}
