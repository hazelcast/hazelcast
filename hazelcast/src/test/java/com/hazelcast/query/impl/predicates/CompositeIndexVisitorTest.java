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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.CompositeValue;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static com.hazelcast.query.Predicates.between;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.greaterEqual;
import static com.hazelcast.query.Predicates.greaterThan;
import static com.hazelcast.query.Predicates.lessEqual;
import static com.hazelcast.query.Predicates.lessThan;
import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static com.hazelcast.query.impl.CompositeValue.NEGATIVE_INFINITY;
import static com.hazelcast.query.impl.CompositeValue.POSITIVE_INFINITY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompositeIndexVisitorTest extends VisitorTestSupport {

    private static final Comparable N_INF = NEGATIVE_INFINITY;
    private static final Comparable P_INF = POSITIVE_INFINITY;

    private CompositeIndexVisitor visitor;
    private Indexes indexes;

    private InternalIndex o123;
    private InternalIndex u321;
    private InternalIndex o567;

    @Before
    public void before() {
        indexes = mock(Indexes.class);

        o123 = mock(InternalIndex.class);
        when(o123.isOrdered()).thenReturn(true);
        when(o123.getComponents()).thenReturn(components("a1", "a2", "a3"));

        u321 = mock(InternalIndex.class);
        when(u321.isOrdered()).thenReturn(false);
        when(u321.getComponents()).thenReturn(components("a3", "a2", "a1"));

        o567 = mock(InternalIndex.class);
        when(o567.isOrdered()).thenReturn(true);
        when(o567.getComponents()).thenReturn(components("a5", "a6", "a7"));

        // needed to test the preference of shorter indexes over longer ones
        InternalIndex o1234 = mock(InternalIndex.class);
        when(o1234.isOrdered()).thenReturn(true);
        when(o1234.getComponents()).thenReturn(components("a1", "a2", "a3", "a4"));

        when(indexes.getCompositeIndexes()).thenReturn(new InternalIndex[]{o1234, o123, u321, o567});

        visitor = new CompositeIndexVisitor();
    }

    private String[] components(String... names) {
        return names;
    }

    @Test
    public void testUnoptimizablePredicates() {
        check(same());
        check(same(), new CustomPredicate());
        check(same(), new CustomPredicate(), equal("a100", 100));
        check(same(), equal("a100", 100), greaterThan("a100", 100));
        check(same(), equal("a100", 100), equal("a101", 101));
        check(same(), greaterThan("a100", 100), lessEqual("a101", 101), equal("a102", 102));

        check(same(), equal("a1", 1));
        check(same(), equal("a1", 1), equal("a1", 11));
        check(same(), greaterThan("a1", 1), lessThan("a1", 1));

        check(same(), equal("a1", 1), between("a100", 100, 200));
        check(same(), equal("a1", 1), equal("a1", 11), between("a100", 100, 200));
    }

    @Test
    public void testOptimizablePredicates() {
        check(range(o123, 1, 2, N_INF, 1, 2, P_INF), equal("a1", 1), equal("a2", 2));
        check(range(o123, 1, 2, P_INF, 1, P_INF, P_INF), equal("a1", 1), greaterThan("a2", 2));
        check(range(o123, 1, 2, N_INF, 1, P_INF, P_INF), equal("a1", 1), greaterEqual("a2", 2));
        check(range(o123, 1, NULL, N_INF, 1, 2, N_INF), equal("a1", 1), lessThan("a2", 2));
        check(range(o123, 1, NULL, N_INF, 1, 2, P_INF), equal("a1", 1), lessEqual("a2", 2));
        check(range(o123, 1, 2, N_INF, 1, 22, P_INF), equal("a1", 1), between("a2", 2, 22));

        check(eq(u321, 3, 2, 1), equal("a1", 1), equal("a2", 2), equal("a3", 3));
        check(eq(o567, 5, 6, 7), equal("a5", 5), equal("a6", 6), equal("a7", 7));

        check(and(eq(u321, 3, 2, 1), ref(3)), equal("a1", 1), equal("a2", 2), equal("a3", 3), new CustomPredicate());
        check(and(eq(u321, 3, 2, 1), ref(3)), equal("a1", 1), equal("a2", 2), equal("a3", 3), greaterThan("a5", 5));
        check(and(eq(o567, 5, 6, 7), ref(3)), equal("a5", 5), equal("a6", 6), equal("a7", 7), greaterThan("a1", 1));
        check(and(range(false, true, o567, 5, 6, NULL, 5, 6, 7), ref(3)), equal("a5", 5), equal("a6", 6), lessEqual("a7", 7),
                greaterThan("a1", 1));

        check(range(false, false, o123, 1, 2, 3, 1, 2, P_INF), equal("a1", 1), equal("a2", 2), greaterThan("a3", 3));
        check(range(true, false, o123, 1, 2, 3, 1, 2, P_INF), equal("a1", 1), equal("a2", 2), greaterEqual("a3", 3));
        check(range(false, false, o123, 1, 2, NULL, 1, 2, 3), equal("a1", 1), equal("a2", 2), lessThan("a3", 3));
        check(range(false, true, o123, 1, 2, NULL, 1, 2, 3), equal("a1", 1), equal("a2", 2), lessEqual("a3", 3));
        check(range(true, true, o123, 1, 2, 3, 1, 2, 33), equal("a1", 1), equal("a2", 2), between("a3", 3, 33));

        check(and(range(o123, 1, 2, N_INF, 1, 2, P_INF), ref(2)), equal("a1", 1), equal("a2", 2), equal("a100", 100));
        check(and(range(false, false, o123, 1, 2, NULL, 1, 2, 3), ref(3)), equal("a1", 1), equal("a2", 2), lessThan("a3", 3),
                equal("a100", 100));
        check(and(range(o123, 1, 2, N_INF, 1, 2, P_INF), ref(2), ref(3)), equal("a1", 1), equal("a2", 2), equal("a100", 100),
                new CustomPredicate());

        check(and(range(o123, 1, 2, N_INF, 1, 2, P_INF), range(o567, 5, 6, N_INF, 5, 6, P_INF)), equal("a1", 1), equal("a2", 2),
                equal("a5", 5), equal("a6", 6));
        check(and(eq(u321, 3, 2, 1), range(o567, 5, 6, N_INF, 5, 6, P_INF)), equal("a1", 1), equal("a2", 2), equal("a3", 3),
                equal("a5", 5), equal("a6", 6));
        check(and(eq(u321, 3, 2, 1), eq(o567, 5, 6, 7)), equal("a1", 1), equal("a2", 2), equal("a3", 3), equal("a5", 5),
                equal("a6", 6), equal("a7", 7));
        check(and(eq(u321, 3, 2, 1), eq(o567, 5, 6, 7), ref(0)), new CustomPredicate(), equal("a1", 1), equal("a2", 2),
                equal("a3", 3), equal("a5", 5), equal("a6", 6), equal("a7", 7));
        check(and(eq(u321, 3, 2, 1), eq(o567, 5, 6, 7), ref(0)), equal("a100", 100), equal("a1", 1), equal("a2", 2),
                equal("a3", 3), equal("a5", 5), equal("a6", 6), equal("a7", 7));
    }

    @Override
    protected Indexes getIndexes() {
        return indexes;
    }

    @Override
    protected Visitor getVisitor() {
        return visitor;
    }

    private Predicate range(InternalIndex index, Comparable... values) {
        return range(false, false, index, values);
    }

    private Predicate range(boolean fromInclusive, boolean toInclusive, InternalIndex index, Comparable... values) {
        assert values.length % 2 == 0;
        return new CompositeRangePredicate(index, new CompositeValue(Arrays.copyOf(values, values.length / 2)), fromInclusive,
                new CompositeValue(Arrays.copyOfRange(values, values.length / 2, values.length)), toInclusive, 0);
    }

    private Predicate eq(InternalIndex index, Comparable... values) {
        return new CompositeEqualPredicate(index, new CompositeValue(values));
    }

}
