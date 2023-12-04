/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.util.function.UnaryOperator.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NonTraversableListTest {
    private List<Integer> list;

    @Before
    public void init() {
        list = new NonTraversableList<>("List items must be finalized");
        list.add(1);
        list.add(2);
        list.add(3);
    }

    @Test
    public void test_size() {
        // Used in classical for loop
        test(() -> 0, i -> i < list.size(), i -> i + 1, i -> list.get(i));
    }

    @Test
    public void test_iterator() {
        // Used in enhanced for loop, spliterator() and stream()
        test(() -> list.iterator(), Iterator::hasNext, identity(), Iterator::next);
    }

    @Test
    public void test_listIterator_forward() {
        test(() -> list.listIterator(), ListIterator::hasNext, identity(), ListIterator::next);
    }

    @Test
    public void test_listIterator_backward() {
        test(() -> list.listIterator(3), ListIterator::hasPrevious, identity(), ListIterator::previous,
                3, 2, 1);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_toArray_withoutGenerator() {
        // Used in copy constructors, bulk add operations and List.copyOf(Collection)
        test(() -> (List<Integer>) (List<?>) List.of(list.toArray()));
    }

    @Test
    public void test_toArray_withGenerator() {
        test(() -> List.of(list.toArray(Integer[]::new)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_clone() {
        test(() -> {
            if (list instanceof NonTraversableList) {
                return (List<Integer>) ((NonTraversableList<Integer>) list).clone();
            }
            if (list instanceof Cloneable) {
                Method cloneMethod = list.getClass().getMethod("clone");
                if (cloneMethod.canAccess(list)) {
                    return (List<Integer>) cloneMethod.invoke(list);
                }
            }
            return list; // Let the non-cloneable lists pass the test
        });
    }

    private <T> void test(Supplier<T> init, Predicate<T> test, UnaryOperator<T> increment,
                          Function<T, Integer> accessor) {
        test(init, test, increment, accessor, 1, 2, 3);
    }

    private <T> void test(Supplier<T> init, Predicate<T> test, UnaryOperator<T> increment,
                          Function<T, Integer> accessor, Integer... values) {
        assertThatThrownBy(() -> test.test(init.get())).hasMessage("List items must be finalized");

        list = ((NonTraversableList<Integer>) list).toReadonlyList();

        List<Integer> clone = new ArrayList<>();
        for (T value = init.get(); test.test(value); value = increment.apply(value)) {
            clone.add(accessor.apply(value));
        }
        assertThat(clone).containsExactly(values);
    }

    private void test(SupplierEx<List<Integer>> getAll) {
        assertThatThrownBy(getAll::get).hasMessage("List items must be finalized");

        list = ((NonTraversableList<Integer>) list).toReadonlyList();

        assertThat(getAll.get()).containsExactly(1, 2, 3);
    }
}
