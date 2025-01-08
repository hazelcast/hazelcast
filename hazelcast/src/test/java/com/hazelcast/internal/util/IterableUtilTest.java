/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class IterableUtilTest {

    private List<Integer> numbers = asList(1, 2, 3);

    @Test
    public void testElementsConverted_whenIterableMapped() {
        Iterable<String> strings = IterableUtil.map(numbers, Object::toString);

        Iterator<String> iter = strings.iterator();
        for (Integer i : numbers) {
            assertEquals(i.toString(), iter.next());
        }
    }

    @Test
    public void testUpToNElement_whenIteratorLimited() {
        Iterator<Integer> limitedIterator = IterableUtil.limit(numbers.iterator(), 2);

        assertEquals(Integer.valueOf(1), limitedIterator.next());
        assertEquals(Integer.valueOf(2), limitedIterator.next());
        assertFalse(limitedIterator.hasNext());
    }

    @Test
    public void testIterableIsEmpty_whenNullUsed() {
        assertEquals(emptyList(), IterableUtil.nullToEmpty(null));
        assertEquals(numbers, IterableUtil.nullToEmpty(numbers));
    }

    @Test
    public void testIterableFilter_when_filter_true() {
        Iterable<Integer> filtered = IterableUtil.filter(numbers, integer -> true);

        assertEquals(numbers.size(), IterableUtil.size(filtered));
    }

    @Test
    public void testIterableFilter_when_filter_false() {
        Iterable<Integer> filtered = IterableUtil.filter(numbers, integer -> false);

        assertEquals(0, IterableUtil.size(filtered));
    }

    @Test
    public void testIterableFilter_calling_iterator_next_advances_state() {
        Iterable<Integer> filtered = IterableUtil.filter(numbers, integer -> true);
        Iterator<Integer> iterator = filtered.iterator();
        for (int i = 0; i < numbers.size(); i++) {
            iterator.next();
        }

        assertEquals(0, IterableUtil.size(filtered));
    }

    @Test
    public void testIterableFilter_calling_iterator_hasNext_not_advances_state_with_false_predicate() {
        Iterable<Integer> filtered = IterableUtil.filter(numbers, integer -> false);
        Iterator<Integer> iterator = filtered.iterator();
        for (int i = 0; i < 2 * numbers.size(); i++) {
            iterator.hasNext();
        }

        assertEquals(0, IterableUtil.size(filtered));
    }

    @Test
    public void testIterableFilter_calling_iterator_hasNext_not_advances_state_with_true_predicate() {
        Iterable<Integer> filtered = IterableUtil.filter(numbers, integer -> true);
        Iterator<Integer> iterator = filtered.iterator();
        for (int i = 0; i < 2 * numbers.size(); i++) {
            iterator.hasNext();
        }

        assertEquals(numbers.size(), IterableUtil.size(filtered));
    }

    @Test(expected = NoSuchElementException.class)
    public void testIterableFilter_calling_iterator_next_more_than_number_of_elements_time() {
        Iterable<Integer> filtered = IterableUtil.filter(numbers, integer -> true);
        Iterator<Integer> iterator = filtered.iterator();
        for (int i = 0; i < numbers.size() + 1; i++) {
            iterator.next();
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_asReadOnlyIterator_throws_exception_when_remove_called() {
        Iterator<Integer> iterator = IterableUtil.asReadOnlyIterator(numbers.iterator());
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }
    }

    @Test
    public void test_asReadOnlyIterator_returns_same_iterator_when_given_iterator_is_read_only() {
        Iterator<Integer> iterator = IterableUtil.asReadOnlyIterator(numbers.iterator());

        assertSame(iterator, IterableUtil.asReadOnlyIterator(iterator));
    }

    @Test
    public void elementNotFound() {
        var list = List.of(1, 2, 3, 4, 5, 6);
        var actual = IterableUtil.skipFirst(list.iterator(), v -> v > 30);
        assertThatThrownBy(actual::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void skipFirst() {
        var list = List.of(1, 2, 3, 4, 5, 6);
        var actual = IterableUtil.skipFirst(list.iterator(), v -> v > 3);
        var expected = List.of(4, 5, 6);
        assertIteratorsEquals(expected, actual);
    }

    @Test
    public void skipFirstAll() {
        var list = List.of(1, 2, 3, 4, 5, 6);
        var actual = IterableUtil.skipFirst(list.iterator(), v -> v > 0);
        assertIteratorsEquals(list, actual);
    }

    @Test
    public void skipFirstNone() {
        var list = List.of(1, 2, 3, 4, 5, 6);
        var actual = IterableUtil.skipFirst(list.iterator(), v -> v > 10);
        assertIteratorsEquals(Collections.emptyList(), actual);
    }

    @Test
    public void skipFirstEmptyCollection() {
        var actual = IterableUtil.skipFirst(Collections.emptyIterator(), v -> false);
        assertIteratorsEquals(Collections.emptyList(), actual);
    }

    @Test
    public void skipFirstNullIterator() {
        assertThatThrownBy(() -> IterableUtil.skipFirst(null, (i) -> false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("iterator cannot be null.");
    }

    @Test
    public void prependNullToEmptyIterator() {
        var actual = IterableUtil.prepend(null, Collections.emptyIterator());
        assertIteratorsEquals(Collections.emptyList(), actual);
    }

    @Test
    public void prependNullIterator() {
        assertThatThrownBy(() -> IterableUtil.prepend(null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("iterator cannot be null.");
    }

    @Test
    public void prependNullElement() {
        List<Integer> list = List.of(1, 2, 3);
        var actual = IterableUtil.prepend(null, list.iterator());
        assertIteratorsEquals(list, actual);
    }

    @Test
    public void prependEmptyIterator() {
        var actual = IterableUtil.prepend(1, Collections.emptyIterator());
        assertIteratorsEquals(List.of(1), actual);
    }

    @Test
    public void prepend() {
        List<Integer> list = List.of(1, 2, 3);
        var actual = IterableUtil.prepend(0, list.iterator());
        assertIteratorsEquals(List.of(0, 1, 2, 3), actual);
    }

    private <T> void assertIteratorsEquals(List<T> expected, Iterator<T> actual) {
        List<T> result = new ArrayList<>();
        actual.forEachRemaining(result::add);
        assertThat(result).isEqualTo(expected);
    }
}
