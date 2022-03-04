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
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryContext.IndexMatchHint;
import com.hazelcast.query.impl.TypeConverters;
import com.hazelcast.query.impl.predicates.VisitorTestSupport.CustomPredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.query.Predicates.alwaysFalse;
import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.in;
import static com.hazelcast.query.Predicates.like;
import static com.hazelcast.query.Predicates.not;
import static com.hazelcast.query.Predicates.notEqual;
import static com.hazelcast.query.Predicates.or;
import static com.hazelcast.query.impl.Indexes.SKIP_PARTITIONS_COUNT_CHECK;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EvaluateVisitorTest {

    private static final Set<Class<? extends Predicate>> EVALUABLE_PREDICATES = new HashSet<>();

    static {
        EVALUABLE_PREDICATES.add(AndPredicate.class);
        EVALUABLE_PREDICATES.add(OrPredicate.class);
        EVALUABLE_PREDICATES.add(NotPredicate.class);

        EVALUABLE_PREDICATES.add(EqualPredicate.class);
        EVALUABLE_PREDICATES.add(NotEqualPredicate.class);
        EVALUABLE_PREDICATES.add(InPredicate.class);
    }

    private EvaluateVisitor visitor = new EvaluateVisitor();
    private Indexes indexes;

    @SuppressWarnings("SuspiciousMethodCalls")
    @Before
    public void before() {
        indexes = mock(Indexes.class);

        InternalIndex bitmapA = mock(InternalIndex.class);
        when(bitmapA.getConverter()).thenReturn(TypeConverters.INTEGER_CONVERTER);
        when(bitmapA.getName()).thenReturn("a");
        when(indexes.matchIndex(eq("a"), any(), eq(IndexMatchHint.EXACT_NAME), eq(SKIP_PARTITIONS_COUNT_CHECK)))
                .then((Answer<InternalIndex>) invocation ->
                        EVALUABLE_PREDICATES.contains(invocation.getArgument(1)) ? bitmapA : null);
        when(indexes.matchIndex(eq("a"), any(), eq(IndexMatchHint.PREFER_UNORDERED), eq(SKIP_PARTITIONS_COUNT_CHECK)))
                .then((Answer<InternalIndex>) invocation ->
                        EVALUABLE_PREDICATES.contains(invocation.getArgument(1)) ? bitmapA : null);

        InternalIndex bitmapB = mock(InternalIndex.class);
        when(bitmapB.getConverter()).thenReturn(TypeConverters.STRING_CONVERTER);
        when(bitmapB.getName()).thenReturn("b");
        when(indexes.matchIndex(eq("b"), any(), eq(IndexMatchHint.EXACT_NAME), eq(SKIP_PARTITIONS_COUNT_CHECK)))
                .then((Answer<InternalIndex>) invocation ->
                        EVALUABLE_PREDICATES.contains(invocation.getArgument(1)) ? bitmapB : null);
        when(indexes.matchIndex(eq("b"), any(), eq(IndexMatchHint.PREFER_UNORDERED), eq(SKIP_PARTITIONS_COUNT_CHECK)))
                .then((Answer<InternalIndex>) invocation ->
                        EVALUABLE_PREDICATES.contains(invocation.getArgument(1)) ? bitmapB : null);

        InternalIndex bitmapNoConverter = mock(InternalIndex.class);
        when(bitmapNoConverter.getName()).thenReturn("nc");
        when(bitmapNoConverter.getConverter()).thenReturn(null);
        when(indexes.matchIndex(eq("nc"), any(), eq(IndexMatchHint.EXACT_NAME), eq(SKIP_PARTITIONS_COUNT_CHECK)))
                .then((Answer<InternalIndex>) invocation ->
                        EVALUABLE_PREDICATES.contains(invocation.getArgument(1)) ? bitmapNoConverter : null);
        when(indexes.matchIndex(eq("nc"), any(), eq(IndexMatchHint.PREFER_UNORDERED), eq(SKIP_PARTITIONS_COUNT_CHECK)))
                .then((Answer<InternalIndex>) invocation ->
                        EVALUABLE_PREDICATES.contains(invocation.getArgument(1)) ? bitmapNoConverter : null);

        InternalIndex bitmapNoSubPredicates = mock(InternalIndex.class);
        when(bitmapNoSubPredicates.getName()).thenReturn("ns");
        when(bitmapNoSubPredicates.getConverter()).thenReturn(TypeConverters.INTEGER_CONVERTER);
        when(indexes.matchIndex(eq("ns"), any(), eq(IndexMatchHint.EXACT_NAME), eq(SKIP_PARTITIONS_COUNT_CHECK)))
                .then((Answer<InternalIndex>) invocation -> {
                    Object clazz = invocation.getArgument(1);
                    if (clazz == AndPredicate.class || clazz == OrPredicate.class || clazz == NotPredicate.class) {
                        return null;
                    } else {
                        return bitmapNoSubPredicates;
                    }
                });
        when(indexes.matchIndex(eq("ns"), any(), eq(IndexMatchHint.PREFER_UNORDERED), eq(SKIP_PARTITIONS_COUNT_CHECK)))
                .then((Answer<InternalIndex>) invocation -> {
                    Object clazz = invocation.getArgument(1);
                    if (clazz == AndPredicate.class || clazz == OrPredicate.class || clazz == NotPredicate.class) {
                        return null;
                    } else {
                        return bitmapNoSubPredicates;
                    }
                });

        visitor = new EvaluateVisitor();
    }

    @Test
    public void testUnoptimizablePredicates() {
        assertNoOptimization(alwaysFalse());
        assertNoOptimization(new CustomPredicate());
        assertNoOptimization(equal("r", 1));
        assertNoOptimization(equal("nc", 1));
        assertNoOptimization(notEqual("r", 1));
        assertNoOptimization(notEqual("nc", 1));
        assertNoOptimization(in("r", 1, 2, 3));
        assertNoOptimization(in("nc", 1, 2, 3));
        assertNoOptimization(and());
        assertNoOptimization(or());
        assertNoOptimization(and(equal("r", 1)));
        assertNoOptimization(and(equal("nc", 1)));
        assertNoOptimization(and(new CustomPredicate()));
        assertNoOptimization(or(equal("noIndex", 1)));
        assertNoOptimization(or(in("noIndex", 1, 2, 5)));
        assertNoOptimization(or(equal("r", 1)));
        assertNoOptimization(or(equal("nc", 1)));
        assertNoOptimization(or(new CustomPredicate(), new CustomPredicate()));
        assertNoOptimization(and(equal("r", 1), equal("noIndex", 1)));
        assertNoOptimization(or(equal("r", 1), equal("noIndex", 1)));
        assertNoOptimization(not(equal("r", 1)));
        assertNoOptimization(not(equal("nc", 1)));
        assertNoOptimization(notEqual("noIndex", 1));
    }

    @Test
    public void testOptimizablePredicates() {
        assertOptimization(and(equal("a", 1)), and(eval(equal("a", 1), "a")));
        assertOptimization(and(equal("a", 1), equal("a", 2)), eval(and(equal("a", 1), equal("a", 2)), "a"));
        assertOptimization(and(equal("a", 1), notEqual("b", 0)), and(eval(equal("a", 1), "a"), eval(notEqual("b", 0), "b")));
        assertOptimization(and(equal("a", 1), notEqual("ns", 0)), and(eval(equal("a", 1), "a"), eval(notEqual("ns", 0), "ns")));
        assertOptimization(and(equal("a", 1), in("ns", 1, 2, 3), notEqual("ns", 0)),
                and(eval(equal("a", 1), "a"), eval(notEqual("ns", 0), "ns"), eval(in("ns", 1, 2, 3), "ns")));
        assertOptimization(and(equal("a", 1), equal("a", 2), like("r", ".*")),
                and(eval(and(equal("a", 1), equal("a", 2)), "a"), like("r", ".*")));
        assertOptimization(and(equal("a", 1), equal("a", 2), equal("ns", 5)),
                and(eval(and(equal("a", 1), equal("a", 2)), "a"), eval(equal("ns", 5), "ns")));
        assertOptimization(and(equal("a", 1), equal("a", 2), equal("b", 5)),
                and(eval(and(equal("a", 1), equal("a", 2)), "a"), eval(equal("b", 5), "b")));

        assertOptimization(or(equal("a", 1)), or(eval(equal("a", 1), "a")));
        assertOptimization(or(equal("a", 1), equal("a", 2)), eval(or(equal("a", 1), equal("a", 2)), "a"));
        assertOptimization(or(equal("a", 1), notEqual("b", 0)), or(eval(equal("a", 1), "a"), eval(notEqual("b", 0), "b")));
        assertOptimization(or(equal("a", 1), notEqual("ns", 0)), or(eval(equal("a", 1), "a"), eval(notEqual("ns", 0), "ns")));
        assertOptimization(or(equal("a", 1), in("ns", 1, 2, 3), notEqual("ns", 0)),
                or(eval(equal("a", 1), "a"), eval(notEqual("ns", 0), "ns"), eval(in("ns", 1, 2, 3), "ns")));
        assertOptimization(or(equal("a", 1), equal("a", 2), like("r", ".*")),
                or(eval(or(equal("a", 1), equal("a", 2)), "a"), like("r", ".*")));
        assertOptimization(or(equal("a", 1), equal("a", 2), equal("ns", 5)),
                or(eval(or(equal("a", 1), equal("a", 2)), "a"), eval(equal("ns", 5), "ns")));
        assertOptimization(or(equal("a", 1), equal("a", 2), equal("b", 5)),
                or(eval(or(equal("a", 1), equal("a", 2)), "a"), eval(equal("b", 5), "b")));

        assertOptimization(not(equal("a", 1)), eval(not(equal("a", 1)), "a"));
        assertOptimization(not(equal("ns", 1)), not(eval(equal("ns", 1), "ns")));

        assertOptimization(and(or(equal("a", 1), equal("b", 2)), equal("a", 3), equal("a", 4)),
                and(or(eval(equal("a", 1), "a"), eval(equal("b", 2), "b")), eval(and(equal("a", 3), equal("a", 4)), "a")));
    }

    private void assertNoOptimization(Predicate original) {
        Predicate actual = optimize(original);
        assertSame(original, actual);
    }

    private void assertOptimization(Predicate original, Predicate expected) {
        Predicate actual = optimize(original);
        assertTrue(original.toString() + " vs " + expected.toString(), homomorphic(original, expected, true));
        assertTrue(original.toString() + " vs " + actual.toString(), homomorphic(original, actual, false));
        assertTrue(expected.toString() + " vs " + actual.toString(), same(expected, actual));
    }

    private static boolean same(Predicate expected, Predicate actual) {
        if (expected.equals(actual)) {
            return true;
        }

        if (expected instanceof EvaluatePredicate && actual instanceof EvaluatePredicate) {
            return same(((EvaluatePredicate) expected).getPredicate(), ((EvaluatePredicate) actual).getPredicate());
        }

        if (expected instanceof AndPredicate && actual instanceof AndPredicate) {
            List<Predicate> expectedSubPredicates = new ArrayList<>(asList(((AndPredicate) expected).predicates));
            List<Predicate> actualSubPredicates = new ArrayList<>(asList(((AndPredicate) actual).predicates));

            for (int i = expectedSubPredicates.size() - 1; i >= 0; --i) {
                for (int j = actualSubPredicates.size() - 1; j >= 0; --j) {
                    if (same(expectedSubPredicates.get(i), actualSubPredicates.get(j))) {
                        expectedSubPredicates.remove(i);
                        actualSubPredicates.remove(j);
                        break;
                    }
                }
            }

            return expectedSubPredicates.isEmpty() && actualSubPredicates.isEmpty();
        }

        if (expected instanceof OrPredicate && actual instanceof OrPredicate) {
            List<Predicate> expectedSubPredicates = new ArrayList<>(asList(((OrPredicate) expected).predicates));
            List<Predicate> actualSubPredicates = new ArrayList<>(asList(((OrPredicate) actual).predicates));

            for (int i = expectedSubPredicates.size() - 1; i >= 0; --i) {
                for (int j = actualSubPredicates.size() - 1; j >= 0; --j) {
                    if (same(expectedSubPredicates.get(i), actualSubPredicates.get(j))) {
                        expectedSubPredicates.remove(i);
                        actualSubPredicates.remove(j);
                        break;
                    }
                }
            }

            return expectedSubPredicates.isEmpty() && actualSubPredicates.isEmpty();
        }

        if (expected instanceof NotPredicate && actual instanceof NotPredicate) {
            return same(((NotPredicate) expected).getPredicate(), ((NotPredicate) actual).getPredicate());
        }

        return false;
    }

    private static boolean homomorphic(Predicate expected, Predicate actual, boolean useEquals) {
        if (expected == actual) {
            return true;
        }

        if (useEquals && expected.equals(actual)) {
            return true;
        }

        if (actual instanceof EvaluatePredicate) {
            return homomorphic(expected, ((EvaluatePredicate) actual).getPredicate(), useEquals);
        }

        if (expected instanceof AndPredicate && actual instanceof AndPredicate) {
            List<Predicate> expectedSubPredicates = new ArrayList<>(asList(((AndPredicate) expected).predicates));
            List<Predicate> actualSubPredicates = new ArrayList<>(asList(((AndPredicate) actual).predicates));

            // First comparison pass.

            for (int i = expectedSubPredicates.size() - 1; i >= 0; --i) {
                for (int j = actualSubPredicates.size() - 1; j >= 0; --j) {
                    if (homomorphic(expectedSubPredicates.get(i), actualSubPredicates.get(j), useEquals)) {
                        expectedSubPredicates.remove(i);
                        actualSubPredicates.remove(j);
                        break;
                    }
                }
            }
            if (expectedSubPredicates.isEmpty() && actualSubPredicates.isEmpty()) {
                return true;
            }

            // Pull up predicates from the nested evaluable "and" predicates.

            for (int i = actualSubPredicates.size() - 1; i >= 0; --i) {
                Predicate actualSubPredicate = actualSubPredicates.get(i);
                if (actualSubPredicate instanceof EvaluatePredicate) {
                    EvaluatePredicate actualEvaluatePredicate = (EvaluatePredicate) actualSubPredicate;
                    if (actualEvaluatePredicate.getPredicate() instanceof AndPredicate) {
                        AndPredicate actualAndPredicate = (AndPredicate) actualEvaluatePredicate.getPredicate();
                        actualSubPredicates.remove(i);
                        actualSubPredicates.addAll(asList(actualAndPredicate.predicates));
                    }
                }
            }

            // Second comparison pass.

            for (int i = expectedSubPredicates.size() - 1; i >= 0; --i) {
                for (int j = actualSubPredicates.size() - 1; j >= 0; --j) {
                    if (homomorphic(expectedSubPredicates.get(i), actualSubPredicates.get(j), useEquals)) {
                        expectedSubPredicates.remove(i);
                        actualSubPredicates.remove(j);
                        break;
                    }
                }
            }
            return expectedSubPredicates.isEmpty() && actualSubPredicates.isEmpty();
        }

        if (expected instanceof OrPredicate && actual instanceof OrPredicate) {
            List<Predicate> expectedSubPredicates = new ArrayList<>(asList(((OrPredicate) expected).predicates));
            List<Predicate> actualSubPredicates = new ArrayList<>(asList(((OrPredicate) actual).predicates));

            // First comparison pass.

            for (int i = expectedSubPredicates.size() - 1; i >= 0; --i) {
                for (int j = actualSubPredicates.size() - 1; j >= 0; --j) {
                    if (homomorphic(expectedSubPredicates.get(i), actualSubPredicates.get(j), useEquals)) {
                        expectedSubPredicates.remove(i);
                        actualSubPredicates.remove(j);
                        break;
                    }
                }
            }
            if (expectedSubPredicates.isEmpty() && actualSubPredicates.isEmpty()) {
                return true;
            }

            // Pull up predicates from the nested evaluable "or" predicates.

            for (int i = actualSubPredicates.size() - 1; i >= 0; --i) {
                Predicate actualSubPredicate = actualSubPredicates.get(i);
                if (actualSubPredicate instanceof EvaluatePredicate) {
                    EvaluatePredicate actualEvaluatePredicate = (EvaluatePredicate) actualSubPredicate;
                    if (actualEvaluatePredicate.getPredicate() instanceof OrPredicate) {
                        OrPredicate actualAndPredicate = (OrPredicate) actualEvaluatePredicate.getPredicate();
                        actualSubPredicates.remove(i);
                        actualSubPredicates.addAll(asList(actualAndPredicate.predicates));
                    }
                }
            }

            // Second comparison pass.

            for (int i = expectedSubPredicates.size() - 1; i >= 0; --i) {
                for (int j = actualSubPredicates.size() - 1; j >= 0; --j) {
                    if (homomorphic(expectedSubPredicates.get(i), actualSubPredicates.get(j), useEquals)) {
                        expectedSubPredicates.remove(i);
                        actualSubPredicates.remove(j);
                        break;
                    }
                }
            }
            return expectedSubPredicates.isEmpty() && actualSubPredicates.isEmpty();
        }

        if (expected instanceof NotPredicate && actual instanceof NotPredicate) {
            return homomorphic(((NotPredicate) expected).getPredicate(), ((NotPredicate) actual).getPredicate(), useEquals);
        }

        return false;
    }

    private Predicate optimize(Predicate input) {
        if (input instanceof VisitablePredicate) {
            return ((VisitablePredicate) input).accept(visitor, indexes);
        } else {
            return input;
        }
    }

    private Predicate eval(Predicate subPredicate, String index) {
        return new EvaluatePredicate(subPredicate, index);
    }

}
