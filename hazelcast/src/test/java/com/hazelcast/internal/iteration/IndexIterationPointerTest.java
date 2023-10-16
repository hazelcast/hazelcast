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

package com.hazelcast.internal.iteration;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.hazelcast.query.impl.OrderedIndexStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Range.atLeast;
import static com.google.common.collect.Range.atMost;
import static com.google.common.collect.Range.closedOpen;
import static com.google.common.collect.Range.greaterThan;
import static com.google.common.collect.Range.lessThan;
import static com.google.common.collect.Range.singleton;
import static com.hazelcast.internal.iteration.IndexIterationPointer.ALL;
import static com.hazelcast.internal.iteration.IndexIterationPointer.ALL_ALT;
import static com.hazelcast.internal.iteration.IndexIterationPointer.IS_NOT_NULL;
import static com.hazelcast.internal.iteration.IndexIterationPointer.IS_NOT_NULL_DESC;
import static com.hazelcast.internal.iteration.IndexIterationPointer.IS_NULL;
import static com.hazelcast.internal.iteration.IndexIterationPointer.IS_NULL_DESC;
import static com.hazelcast.internal.iteration.IndexIterationPointer.create;
import static com.hazelcast.internal.iteration.IndexIterationPointer.normalizePointers;
import static com.hazelcast.internal.iteration.IndexIterationPointer.overlapsOrdered;
import static com.hazelcast.internal.iteration.IndexIterationPointer.union;
import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexIterationPointerTest {

    // use Guava's nice Range DSL
    // produces ranges that do not include NULL unless NULL is explicitly used as bound
    public static <C extends Comparable> IndexIterationPointer pointer(com.google.common.collect.Range<C> r, boolean descending) {
        return create(
                r.hasLowerBound() ? r.lowerEndpoint() : NULL, r.hasLowerBound() && r.lowerBoundType() == BoundType.CLOSED,
                r.hasUpperBound() ? r.upperEndpoint() : null, r.hasUpperBound() && r.upperBoundType() == BoundType.CLOSED,
                descending, null
        );
    }

    public static <C extends Comparable> IndexIterationPointer pointer(com.google.common.collect.Range<C> r) {
        return pointer(r, false);
    }

    @Test
    void createSingleton() {
        IndexIterationPointer singleton = create(5, true, 5, true, false, null);
        assertThat(singleton.getFrom()).isEqualTo(singleton.getTo()).isEqualTo(5);
        assertTrue(singleton.isFromInclusive());
        assertTrue(singleton.isToInclusive());
        assertFalse(singleton.isDescending());
    }

    @Test
    void createBadSingleton() {
        assertThatThrownBy(() -> create(5, true, 5, false, false, null))
                .isInstanceOf(AssertionError.class).hasMessageContaining("Point lookup limits must be all inclusive");
        assertThatThrownBy(() -> create(5, false, 5, true, false, null))
                .isInstanceOf(AssertionError.class).hasMessageContaining("Point lookup limits must be all inclusive");
        assertThatThrownBy(() -> create(5, false, 5, false, false, null))
                .isInstanceOf(AssertionError.class).hasMessageContaining("Point lookup limits must be all inclusive");
    }

    @Test
    void overlapsOrderedSingleton() {
        assertTrue(overlapsOrdered(pointer(singleton(5)), pointer(singleton(5)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR), "singleton value should overlap with itself");
        assertFalse(overlapsOrdered(pointer(singleton(5)), pointer(singleton(6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR), "singleton value should not overlap with different singleton");
        assertFalse(overlapsOrdered(pointer(singleton(5), true), pointer(singleton(6), true),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR), "singleton value should not overlap with different singleton");
    }

    @Test
    void overlapsOrderedRanges() {
        // ranges unbounded on the same side
        assertTrue(overlapsOrdered(pointer(lessThan(5)), pointer(lessThan(5)), OrderedIndexStore.SPECIAL_AWARE_COMPARATOR));
        assertTrue(overlapsOrdered(pointer(lessThan(5)), pointer(lessThan(6)), OrderedIndexStore.SPECIAL_AWARE_COMPARATOR));

        assertTrue(overlapsOrdered(pointer(greaterThan(5)), pointer(greaterThan(5)), OrderedIndexStore.SPECIAL_AWARE_COMPARATOR));
        assertTrue(overlapsOrdered(pointer(greaterThan(5)), pointer(greaterThan(6)), OrderedIndexStore.SPECIAL_AWARE_COMPARATOR));

        // adjacent ranges unbounded on different sides
        assertFalse(overlapsOrdered(pointer(lessThan(5)), pointer(greaterThan(5)), OrderedIndexStore.SPECIAL_AWARE_COMPARATOR),
                "Ranges with the same open end should not be adjacent");
        assertTrue(overlapsOrdered(pointer(lessThan(5)), pointer(atLeast(5)), OrderedIndexStore.SPECIAL_AWARE_COMPARATOR));
        assertTrue(overlapsOrdered(pointer(atMost(5)), pointer(greaterThan(5)), OrderedIndexStore.SPECIAL_AWARE_COMPARATOR));

        // overlapping ranges unbounded on different sides
        assertTrue(overlapsOrdered(pointer(lessThan(6)), pointer(greaterThan(5)), OrderedIndexStore.SPECIAL_AWARE_COMPARATOR));
        assertTrue(overlapsOrdered(pointer(lessThan(6)), pointer(atLeast(5)), OrderedIndexStore.SPECIAL_AWARE_COMPARATOR));
        assertTrue(overlapsOrdered(pointer(atMost(6)), pointer(greaterThan(5)), OrderedIndexStore.SPECIAL_AWARE_COMPARATOR));
        assertTrue(overlapsOrdered(pointer(atMost(6)), pointer(atLeast(5)), OrderedIndexStore.SPECIAL_AWARE_COMPARATOR));

        // non-overlapping ranges unbounded on different sides
        assertFalse(overlapsOrdered(pointer(lessThan(5)), pointer(greaterThan(6)), OrderedIndexStore.SPECIAL_AWARE_COMPARATOR));
        assertFalse(overlapsOrdered(pointer(lessThan(5)), pointer(atLeast(6)), OrderedIndexStore.SPECIAL_AWARE_COMPARATOR));
        assertFalse(overlapsOrdered(pointer(atMost(5)), pointer(greaterThan(6)), OrderedIndexStore.SPECIAL_AWARE_COMPARATOR));
        assertFalse(overlapsOrdered(pointer(atMost(5)), pointer(atLeast(6)), OrderedIndexStore.SPECIAL_AWARE_COMPARATOR));
    }

    @Test
    void overlapsOrderedSingletonValidation() {
        assertThatThrownBy(() -> overlapsOrdered(pointer(singleton(6)), pointer(singleton(5)), OrderedIndexStore.SPECIAL_AWARE_COMPARATOR))
                .isInstanceOf(AssertionError.class).hasMessageContaining("Pointers must be ordered");
        assertThatThrownBy(() -> overlapsOrdered(pointer(singleton(6), true), pointer(singleton(5), true),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR))
                .isInstanceOf(AssertionError.class).hasMessageContaining("Pointers must be ordered");
    }

    @Test
    void createIsNull() {
        IndexIterationPointer singleton = IS_NULL;
        assertThat(singleton.getFrom()).isEqualTo(singleton.getTo()).isEqualTo(NULL);
        assertTrue(singleton.isFromInclusive());
        assertTrue(singleton.isToInclusive());
        assertFalse(singleton.isDescending());
    }

    @Test
    void overlapsIsNull() {
        assertTrue(overlapsOrdered(IS_NULL, IS_NULL,
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR), "IS NULL should overlap with itself");
        assertTrue(overlapsOrdered(IS_NULL_DESC, IS_NULL_DESC,
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR), "IS NULL should overlap with itself");

        assertFalse(overlapsOrdered(IS_NULL, pointer(singleton(5)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR), "IS NULL should not overlap with singleton");
        assertFalse(overlapsOrdered(IS_NULL_DESC, pointer(singleton(5), true),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR), "IS NULL should not overlap with singleton");

        assertTrue(overlapsOrdered(IS_NULL, IS_NOT_NULL,
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR), "IS NULL should overlap with NOT NULL (they are adjacent)");
        assertTrue(overlapsOrdered(IS_NULL_DESC, IS_NOT_NULL_DESC,
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR), "IS NULL should overlap with NOT NULL (they are adjacent)");
    }

    @Test
    void isAll() {
        assertFalse(pointer(singleton(5)).isAll());
        assertFalse(IS_NULL.isAll());
        assertFalse(IS_NOT_NULL.isAll());
        assertTrue(ALL.isAll());
        assertTrue(ALL_ALT.isAll());
    }

    @Test
    void unionSpecialValues() {
        // null/not null combinations
        assertThat(union(IS_NULL, IS_NULL,
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(IS_NULL);
        assertThat(union(IS_NOT_NULL, IS_NOT_NULL,
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(IS_NOT_NULL);
        assertTrue(union(IS_NULL, IS_NOT_NULL,
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR).isAll());

        // all + something = all
        assertTrue(union(ALL, IS_NULL,
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR).isAll());
        assertTrue(union(ALL, IS_NOT_NULL,
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR).isAll());
        assertTrue(union(ALL, pointer(singleton(5)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR).isAll());
        assertTrue(union(ALL, pointer(lessThan(5)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR).isAll());
        assertTrue(union(ALL, pointer(atMost(5)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR).isAll());
        assertTrue(union(ALL, pointer(atLeast(5)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR).isAll());
        assertTrue(union(ALL, pointer(greaterThan(5)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR).isAll());
        assertTrue(union(ALL, ALL,
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR).isAll());
    }

    @Test
    void unionAdjacentSingletonRange() {
        assertThat(union(pointer(singleton(5)), pointer(singleton(5)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(pointer(singleton(5)));

        assertThat(union(pointer(singleton(5)), pointer(greaterThan(5)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(pointer(atLeast(5)));
        // reverse order is also allowed
        assertThat(union(pointer(greaterThan(5)), pointer(singleton(5)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(pointer(atLeast(5)));

        assertThat(union(pointer(singleton(5)), pointer(lessThan(5)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(pointer(atMost(5)));
        // reverse order is also allowed
        assertThat(union(pointer(lessThan(5)), pointer(singleton(5)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(pointer(atMost(5)));
    }

    @Test
    void unionRange() {
        assertThat(union(pointer(lessThan(5)), pointer(lessThan(6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(pointer(lessThan(6)));
        assertThat(union(pointer(lessThan(5)), pointer(atMost(6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(pointer(atMost(6)));
        assertThat(union(pointer(atMost(5)), pointer(atMost(6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(pointer(atMost(6)));
        assertThat(union(pointer(atMost(5)), pointer(lessThan(6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(pointer(lessThan(6)));

        assertThat(union(pointer(greaterThan(5)), pointer(greaterThan(6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(pointer(greaterThan(5)));
        assertThat(union(pointer(greaterThan(5)), pointer(atLeast(6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(pointer(greaterThan(5)));
        assertThat(union(pointer(atLeast(5)), pointer(atLeast(6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(pointer(atLeast(5)));
        assertThat(union(pointer(atLeast(5)), pointer(greaterThan(6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(pointer(atLeast(5)));
    }

    @Test
    void unionRangeToAll() {
        // union of { > X } and { < X + NULLs }
        assertThat(union(pointer(greaterThan(5)), pointer(Range.closedOpen(NULL, 6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR).isAll()).isTrue();
        assertThat(union(pointer(atLeast(5)), pointer(Range.closedOpen(NULL, 6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR).isAll()).isTrue();
        assertThat(union(pointer(atLeast(5)), pointer(Range.closed(NULL, 6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR).isAll()).isTrue();
        assertThat(union(pointer(greaterThan(5)), pointer(Range.closed(NULL, 6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR).isAll()).isTrue();

        assertThat(union(pointer(greaterThan(5)), pointer(lessThan(6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(IS_NOT_NULL);
        assertThat(union(pointer(atLeast(5)), pointer(lessThan(6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(IS_NOT_NULL);
        assertThat(union(pointer(atLeast(5)), pointer(atMost(6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(IS_NOT_NULL);
        assertThat(union(pointer(greaterThan(5)), pointer(atMost(6)),
                OrderedIndexStore.SPECIAL_AWARE_COMPARATOR)).isEqualTo(IS_NOT_NULL);
    }

    private static <T> List<T> arrayListOf(T... elements) {
        return new ArrayList<>(List.of(elements));
    }

    @Test
    void normalizePointersMerge() {
        assertThat(normalizePointers(arrayListOf(
                pointer(singleton(5)), pointer(singleton(5))), false))
                .as("Should merge overlapping ranges")
                .containsExactly(pointer(singleton(5)));

        assertThat(normalizePointers(arrayListOf(
                pointer(singleton(5)), pointer(singleton(6))), false))
                .as("Should not merge non overlapping ranges")
                .containsExactly(pointer(singleton(5)), pointer(singleton(6)));

        assertThat(normalizePointers(arrayListOf(
                pointer(singleton(6), true), pointer(singleton(5), true)), true))
                .as("Should not merge non overlapping desc ranges")
                .containsExactly(pointer(singleton(6), true), pointer(singleton(5), true));

        assertThat(normalizePointers(arrayListOf(
                pointer(lessThan(2)), pointer(lessThan(5))), false))
                .as("Should merge ranges in correct order")
                .containsExactly(pointer(lessThan(5)));

        assertThat(normalizePointers(arrayListOf(
                pointer(lessThan(5)), pointer(lessThan(2))), false))
                .as("Should merge ranges in wrong order")
                .containsExactly(pointer(lessThan(5)));
    }

    @Test
    void normalizePointersOrder() {
        assertThat(normalizePointers(arrayListOf(
                pointer(singleton(6)), pointer(singleton(5))), false))
                .as("Should order and not merge non overlapping ranges")
                .containsExactly(pointer(singleton(5)), pointer(singleton(6)));

        assertThat(normalizePointers(arrayListOf(
                pointer(singleton(6)), pointer(lessThan(5))), false))
                .as("Should order and not merge non overlapping ranges")
                .containsExactly(pointer(lessThan(5)), pointer(singleton(6)));

        assertThat(normalizePointers(arrayListOf(
                pointer(greaterThan(6)), pointer(singleton(5))), false))
                .as("Should order and not merge non overlapping ranges")
                .containsExactly(pointer(singleton(5)), pointer(greaterThan(6)));

        assertThat(normalizePointers(arrayListOf(
                pointer(singleton(5), true), pointer(singleton(6), true)), true))
                .as("Should order and not merge non overlapping desc ranges")
                .containsExactly(pointer(singleton(6), true), pointer(singleton(5), true));
    }

    @Test
    void normalizePointersMergeIsNullWithLessThan() {
        // due to NULL ordering NULL and < X pointers can be merged

        assertThat(normalizePointers(arrayListOf(
                pointer(lessThan(6)), IS_NULL), false))
                .as("IS NULL should be merged with less than")
                .containsExactly(pointer(closedOpen(NULL, 6)));

        assertThat(normalizePointers(arrayListOf(
                pointer(greaterThan(6)), IS_NULL), false))
                .as("IS NULL should not be merged with greater than")
                .containsExactly(IS_NULL, pointer(greaterThan(6)));
    }

    @Test
    void normalizePointersMany() {
        // some cases for partial reduction of number of pointers
        assertThat(normalizePointers(arrayListOf(
                pointer(singleton(6)), pointer(singleton(5)), IS_NULL), false))
                .containsExactly(IS_NULL, pointer(singleton(5)), pointer(singleton(6)));

        assertThat(normalizePointers(arrayListOf(
                pointer(singleton(6)), pointer(singleton(5)), IS_NOT_NULL), false))
                .containsExactly(IS_NOT_NULL);

        assertThat(normalizePointers(arrayListOf(
                pointer(greaterThan(5)), pointer(singleton(6)), IS_NULL), false))
                .containsExactly(IS_NULL, pointer(greaterThan(5)));

    }
}
