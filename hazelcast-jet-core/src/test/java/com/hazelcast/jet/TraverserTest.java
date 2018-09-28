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

package com.hazelcast.jet;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.Traversers.traverseItems;
import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TraverserTest {

    @Test
    public void smokeTest_map() {
        check(traverser().map(identity()),
                0, 1, 2);
        check(traverser().map(i -> i + 1),
                1, 2, 3);
        check(traverser().map(i -> i % 2 != 0 ? null : i),
                0, 2);
        check(traverser().map(i -> null)
                );
        check(traverser().map(i -> i == 0 ? null : i),
                1, 2);
        check(traverser().map(i -> i != 0 ? null : i),
                0);
    }

    @Test
    public void smokeTest_filter() {
        check(traverser().filter(i -> true),
                0, 1, 2);
        check(traverser().filter(i -> false)
                );
        check(traverser().filter(i -> i != 1),
                0, 2);
        check(traverser().filter(i -> i == 1),
                1);
        check(traverser().filter(i -> i != 0),
                1, 2);
        check(traverser().filter(i -> i != 2),
                0, 1);
    }

    @Test
    public void smokeTest_takeWhile() {
        check(traverser().takeWhile(i -> false)
                );
        check(traverser().takeWhile(i -> true),
                0, 1, 2);
        check(traverser().takeWhile(i -> i < 3),
                0, 1, 2);
        check(traverser().takeWhile(i -> i < 2),
                0, 1);
        check(traverser().takeWhile(i -> i < 1),
                0);
        check(traverser().takeWhile(i -> i < 0)
                );
    }

    @Test
    public void smokeTest_dropWhile() {
        check(traverser().dropWhile(i -> true)
                );
        check(traverser().dropWhile(i -> false),
                0, 1, 2);
        check(traverser().dropWhile(i -> i < 3)
                );
        check(traverser().dropWhile(i -> i < 2),
                2);
        check(traverser().dropWhile(i -> i < 1),
                1, 2);
        check(traverser().dropWhile(i -> i < 0),
                0, 1, 2);
    }

    @Test
    public void smokeTest_append() {
        check(traverser().append(5),
                0, 1, 2, 5);
        check(traverser().append(5).append(6),
                0, 1, 2, 5, 6);
        check(Traversers.empty().append(0),
                0);
    }

    @Test
    public void smokeTest_prepend() {
        check(traverser().prepend(5),
                5, 0, 1, 2);
        check(traverser().prepend(5).prepend(6),
                6, 5, 0, 1, 2);
        check(Traversers.empty().prepend(0),
                0);
    }

    @Test
    public void smokeTest_flatMap() {
        check(traverser().flatMap(i -> traverser()),
                0, 1, 2, 0, 1, 2, 0, 1, 2);
        check(traverser().flatMap(i -> Traversers.empty())
                );
        check(traverser().flatMap(i -> traverser().takeWhile(j -> j < i)),
                0, 0, 1); // 0 -> {}, 1 -> {0}, 2 -> {0, 1} ==> {0, 0, 1}
    }

    @Test
    public void smokeTest_peek() {
        List<Integer> peekList = new ArrayList<>();
        check(traverser().peek(peekList::add),
                0, 1, 2);
        assertEquals(asList(0, 1, 2), peekList);
        peekList.clear();

        // check that it is not peeking the appended item
        check(traverser().peek(peekList::add).append(4),
                0, 1, 2, 4);
        assertEquals(asList(0, 1, 2), peekList);
        peekList.clear();

        // check that it is not peeking the prepended item
        check(traverser().peek(peekList::add).prepend(4),
                4, 0, 1, 2);
        assertEquals(asList(0, 1, 2), peekList);
        peekList.clear();

        // check that it is peeking the items filtered-out after peek()
        check(traverser().peek(peekList::add).filter(i -> i == 0),
                0);
        assertEquals(asList(0, 1, 2), peekList);
        peekList.clear();
    }

    @Test
    public void smokeTest_onFirstNull() {
        boolean[] actionCalled = {false};
        Traverser<Integer> t = traverser()
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
        assertNull(t.next()); // this will file inside the action if called duplicately
    }

    @Test
    public void when_allFilteredOut_then_onFirstNullCalledImmediately() {
        boolean[] actionCalled = {false};
        Traverser<Integer> t = traverser()
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
        Traverser<Integer> t = traverser()
                .map(i -> (Integer) null)
                .onFirstNull(() -> {
                    assert !actionCalled[0] : "action already called";
                    actionCalled[0] = true;
                });

        assertFalse(actionCalled[0]);
        assertNull(t.next());
        assertTrue(actionCalled[0]);
    }

    private static Traverser<Integer> traverser() {
        return traverseItems(0, 1, 2);
    }

    @SafeVarargs
    private static <T> void check(Traverser<T> t, T... expected) {
        List<T> list = new ArrayList<>();
        for (T item; (item = t.next()) != null; ) {
            list.add(item);
        }
        assertEquals(asList(expected), list);
        // after first null there should be only nulls
        assertNull(t.next());
        assertNull(t.next());
    }
}
