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
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.query.Predicates.alwaysFalse;
import static com.hazelcast.query.Predicates.between;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.greaterEqual;
import static com.hazelcast.query.Predicates.greaterThan;
import static com.hazelcast.query.Predicates.lessEqual;
import static com.hazelcast.query.Predicates.lessThan;
import static com.hazelcast.query.Predicates.like;
import static com.hazelcast.query.impl.TypeConverters.INTEGER_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.STRING_CONVERTER;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RangeVisitorTest extends VisitorTestSupport {

    private RangeVisitor visitor;
    private Indexes indexes;

    @Before
    public void before() {
        indexes = mock(Indexes.class);
        when(indexes.getConverter("age")).thenReturn(INTEGER_CONVERTER);
        when(indexes.getConverter("name")).thenReturn(STRING_CONVERTER);
        when(indexes.getConverter("noConverter")).thenReturn(null);
        when(indexes.getConverter("collection[any]")).thenReturn(INTEGER_CONVERTER);
        when(indexes.getConverter("collection[0]")).thenReturn(INTEGER_CONVERTER);

        visitor = new RangeVisitor();
    }

    @Test
    public void testUnoptimizablePredicates() {
        check(same());
        check(same(), new CustomPredicate());
        check(same(), equal("age", 0));
        check(same(), equal("unindexed", 0), greaterEqual("unindexed", 0));
        check(same(), equal("noConverter", 0), lessEqual("noConverter", 0));
        check(same(), equal("name", "Alice"), like("name", "Alice"));
        check(same(), equal("age", 0), equal("name", "Alice"));
        check(same(), equal("age", 0), equal("name", "Alice"), greaterEqual("unindexed", true));
        check(same(), between("name", "Alice", "Bob"));
        check(same(), equal("collection[any]", 0));
        check(same(), equal("collection[any]", 0), equal("collection[any]", 1));
    }

    @Test
    public void testUnsatisfiablePredicates() {
        check(alwaysFalse(), alwaysFalse());
        check(alwaysFalse(), alwaysFalse(), equal("name", "Alice"), equal("name", "Alice"));
        check(alwaysFalse(), new CustomPredicate(), alwaysFalse());
        check(alwaysFalse(), equal("name", "Alice"), equal("name", "Bob"));
        check(alwaysFalse(), new CustomPredicate(), equal("name", "Alice"), equal("name", "Bob"));
        check(alwaysFalse(), equal("age", 10), greaterThan("age", "10"));
        check(alwaysFalse(), lessEqual("age", "8"), equal("age", 9));
        check(alwaysFalse(), lessThan("age", 9), equal("age", 9.0));
        check(alwaysFalse(), greaterEqual("age", "10"), lessThan("age", 10.0));
        check(alwaysFalse(), greaterThan("age", "10"), lessThan("age", 0));
        check(alwaysFalse(), equal("name", "Alice"), equal("name", "Bob"), greaterEqual("age", "10"), equal("age", 9));
        check(alwaysFalse(), equal("collection[0]", 0), equal("collection[0]", 1));
    }

    @Test
    public void testOptimizablePredicates() {
        check(equal("age", 1), equal("age", 1), equal("age", "1"));
        check(greaterThan("age", 1), greaterEqual("age", 1.0), greaterThan("age", "1"));
        check(lessThan("age", 10), lessEqual("age", 10.0), lessThan("age", "10"));
        check(greaterEqual("age", 10), greaterEqual("age", 10.0), greaterThan("age", "1"));
        check(lessEqual("age", 10), lessEqual("age", 10.0), lessThan("age", "100"));
        check(greaterThan("age", 1), greaterThan("age", 1.0), greaterThan("age", "1"));
        check(greaterThan("age", 1), greaterThan("age", -100), greaterThan("age", 1));
        check(lessThan("age", 10), lessThan("age", 10.0), lessThan("age", "10"));
        check(lessThan("age", 10), lessThan("age", 10.0), lessThan("age", "100"));

        check(between("age", 1, 10), greaterEqual("age", 1), lessEqual("age", 10));
        check(new BoundedRangePredicate("age", 1, false, 10, false), greaterThan("age", 1), lessThan("age", 10));
        check(new BoundedRangePredicate("age", 1, true, 10, false), greaterEqual("age", 1), lessThan("age", 10));
        check(new BoundedRangePredicate("age", 1, false, 10, true), greaterThan("age", 1), lessEqual("age", 10));

        check(and(ref(0), equal("age", 1)), equal("name", "Alice"), equal("age", 1), equal("age", "1"));
        check(and(ref(0), equal("age", 1)), new CustomPredicate(), equal("age", 1), equal("age", "1"));
        check(and(ref(0), ref(1), equal("age", 1)), new CustomPredicate(), equal("unindexed", true), equal("age", 1),
                equal("age", "1"));

        check(equal("collection[0]", 1), equal("collection[0]", 1), equal("collection[0]", "1"));
    }

    @Test
    public void testNullsHandling() {
        check(alwaysFalse(), equal("name", "Alice"), equal("name", null));
        check(alwaysFalse(), lessThan("name", "Bob"), equal("name", null));
        check(same(), equal("name", null));
        check(same(), equal("name", null), equal("age", null));
        check(equal("name", null), equal("name", null), equal("name", null));
    }

    @Test
    public void testDuplicatesHandling() {
        Predicate duplicate = new CustomPredicate();
        check(same(), duplicate, duplicate);
        check(same(), duplicate, duplicate, equal("name", "Alice"));
        check(and(ref(0), ref(1), equal("name", "Alice")), duplicate, duplicate, equal("name", "Alice"), equal("name", "Alice"));

        duplicate = equal("unindexed", 1);
        check(same(), duplicate, duplicate);
        check(same(), duplicate, duplicate, equal("name", "Alice"));
        check(and(ref(0), ref(1), equal("name", "Alice")), duplicate, duplicate, equal("name", "Alice"), equal("name", "Alice"));
    }

    @Test
    public void testBetweenPredicateOptimization() {
        BetweenPredicate predicate = new BetweenPredicate("age", 20, 10);
        assertSame(Predicates.alwaysFalse(), visitor.visit(predicate, indexes));

        predicate = new BetweenPredicate("noConverter", 20, 10);
        assertSame(predicate, visitor.visit(predicate, indexes));

        predicate = new BetweenPredicate("unindexed", "20", "10");
        assertSame(predicate, visitor.visit(predicate, indexes));

        predicate = new BetweenPredicate("age", "10", 20);
        assertSame(predicate, visitor.visit(predicate, indexes));

        predicate = new BetweenPredicate("age", "10", "10");
        assertEqualPredicate(new EqualPredicate("age", 10), visitor.visit(predicate, indexes));
    }

    @Override
    protected Indexes getIndexes() {
        return indexes;
    }

    @Override
    protected Visitor getVisitor() {
        return visitor;
    }

}
