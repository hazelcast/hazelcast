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
        new Traversal<Integer>()
                .initialize(() -> 0)
                .test(i -> i < list.size())
                .get(i -> list.get(i))
                .increment(i -> i + 1)
                .run();
    }

    @Test
    public void test_iterator() {
        // Used in enhanced for loop, spliterator() and stream()
        new Traversal<Iterator<Integer>>()
                .initialize(() -> list.iterator())
                .test(Iterator::hasNext)
                .get(Iterator::next)
                .run();
    }

    @Test
    public void test_listIterator_forward() {
        new Traversal<ListIterator<Integer>>()
                .initialize(() -> list.listIterator())
                .test(ListIterator::hasNext)
                .get(ListIterator::next)
                .run();
    }

    @Test
    public void test_listIterator_backward() {
        new Traversal<ListIterator<Integer>>()
                .initialize(() -> list.listIterator(3))
                .test(ListIterator::hasPrevious)
                .get(ListIterator::previous)
                .expect(3, 2, 1)
                .run();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_toArray_withoutGenerator() {
        // Used in copy constructors, bulk add operations and List.copyOf(Collection)
        new BulkGet<Object[]>()
                .getAll(() -> list.toArray())
                .convert(array -> (List<Integer>) (List<?>) List.of(array))
                .run();
    }

    @Test
    public void test_toArray_withGenerator() {
        new BulkGet<Integer[]>()
                .getAll(() -> list.toArray(Integer[]::new))
                .convert(List::of)
                .run();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_clone() {
        new BulkGet<List<Integer>>()
                .getAll(() -> {
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
                }).run();
    }

    private abstract class TestCase<T extends TestCase<T>> {
        private Integer[] expectedValues = {1, 2, 3};

        @SuppressWarnings("unchecked")
        T expect(Integer... values) {
            expectedValues = values;
            return (T) this;
        }

        protected abstract List<Integer> getClone();

        void run() {
            assertThatThrownBy(this::getClone).hasMessage("List items must be finalized");

            list = ((NonTraversableList<Integer>) list).toReadonlyList();

            assertThat(getClone()).containsExactly(expectedValues);
        }
    }

    private class Traversal<T> extends TestCase<Traversal<T>> {
        private Supplier<T> init;
        private Predicate<T> test;
        private Function<T, Integer> get;
        private UnaryOperator<T> increment = identity();

        Traversal<T> initialize(Supplier<T> init) {
            this.init = init;
            return this;
        }

        Traversal<T> test(Predicate<T> test) {
            this.test = test;
            return this;
        }

        Traversal<T> get(Function<T, Integer> get) {
            this.get = get;
            return this;
        }

        Traversal<T> increment(UnaryOperator<T> increment) {
            this.increment = increment;
            return this;
        }

        @Override
        protected List<Integer> getClone() {
            List<Integer> clone = new ArrayList<>();
            for (T value = init.get(); test.test(value); value = increment.apply(value)) {
                clone.add(get.apply(value));
            }
            return clone;
        }
    }

    private class BulkGet<T> extends TestCase<BulkGet<T>> {
        private SupplierEx<T> getAll;
        private Function<T, List<Integer>> convert = List.class::cast;

        BulkGet<T> getAll(SupplierEx<T> getAll) {
            this.getAll = getAll;
            return this;
        }

        BulkGet<T> convert(Function<T, List<Integer>> convert) {
            this.convert = convert;
            return this;
        }

        @Override
        protected List<Integer> getClone() {
            return convert.apply(getAll.get());
        }
    }
}
