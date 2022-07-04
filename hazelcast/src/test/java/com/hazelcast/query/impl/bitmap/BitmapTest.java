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

package com.hazelcast.query.impl.bitmap;

import com.hazelcast.internal.monitor.impl.GlobalIndexOperationStats;
import com.hazelcast.internal.monitor.impl.IndexOperationStats;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.in;
import static com.hazelcast.query.Predicates.not;
import static com.hazelcast.query.Predicates.notEqual;
import static com.hazelcast.query.Predicates.or;
import static com.hazelcast.query.impl.TypeConverters.INTEGER_CONVERTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BitmapTest {

    private static final long COUNT = 1000;

    private static final Predicate[] actualQueries;

    static {
        actualQueries = new Predicate[10];
        actualQueries[0] = notEqual("a", "0");
        actualQueries[1] = equal("a", 1L);
        actualQueries[2] = equal("a", 2);
        actualQueries[3] = or(equal("a", 1L), equal("a", 2));
        actualQueries[4] = in("a", 3, 4);
        actualQueries[5] = not(in("a", 3, "4"));

        // all together
        actualQueries[6] = and(notEqual("a", "0"), or(equal("a", 1L), equal("a", 2)), not(in("a", 3, "4")));

        // negate the previous one
        actualQueries[7] = not(and(notEqual("a", "0"), or(equal("a", 1L), equal("a", 2)), not(in("a", 3, "4"))));

        // single-predicate and/or
        actualQueries[8] = or(equal("a", 1.0D));
        actualQueries[9] = and(equal("a", 1.0F));
    }

    private final ExpectedQuery[] expectedQueries;

    {
        expectedQueries = new ExpectedQuery[10];
        expectedQueries[0] = new ExpectedQuery(value -> !bit(0, value));
        expectedQueries[1] = new ExpectedQuery(value -> bit(1, value));
        expectedQueries[2] = new ExpectedQuery(value -> bit(2, value));
        expectedQueries[3] = new ExpectedQuery(value -> bit(1, value) || bit(2, value));
        expectedQueries[4] = new ExpectedQuery(value -> bit(3, value) || bit(4, value));
        expectedQueries[5] = new ExpectedQuery(value -> !(bit(3, value) || bit(4, value)));
        expectedQueries[6] = new ExpectedQuery(
                value -> !bit(0, value) && (bit(1, value) || bit(2, value)) && !(bit(3, value) || bit(4, value)));
        expectedQueries[7] = new ExpectedQuery(
                value -> !(!bit(0, value) && (bit(1, value) || bit(2, value)) && !(bit(3, value) || bit(4, value))));
        expectedQueries[8] = new ExpectedQuery(value -> bit(1, value));
        expectedQueries[9] = new ExpectedQuery(value -> bit(1, value));
    }

    private final Bitmap<String> bitmap = new Bitmap<>();

    private final IndexOperationStats operationStats = new GlobalIndexOperationStats();

    @Test
    public void testInsertUpdateRemove() {
        // insert
        for (long i = 0; i < COUNT; ++i) {
            insert(i, i);
            verify();
        }
        // update inserted
        for (long i = 0; i < COUNT; ++i) {
            update(i, i, i + 1);
            verify();
        }
        // update some nonexistent records
        for (long i = COUNT; i < COUNT * 2; ++i) {
            update(i, i, i + 1);
            verify();
        }
        // remove existed updated records
        for (long i = 0; i < COUNT; ++i) {
            remove(i, i + 1);
            verify();
        }
        // remove some nonexistent records
        for (long i = COUNT * 3; i < COUNT * 4; ++i) {
            remove(i, i);
            verify();
        }
        // remove nonexistent "updated" records
        for (long i = COUNT; i < COUNT * 2; ++i) {
            remove(i, i + 1);
            verify();
        }
    }

    @Test
    public void testClear() {
        // insert
        for (long i = 0; i < COUNT; ++i) {
            insert(i, i);
            verify();
        }
        clear();
        verify();

        // reinsert
        for (long i = 0; i < COUNT; ++i) {
            insert(i, i);
            verify();
        }
        clear();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnexpectedPredicate() {
        bitmap.evaluate(Predicates.like("a", "b"), INTEGER_CONVERTER);
    }

    private void insert(long key, long value) {
        bitmap.insert(values(value), key, Long.toString(key), operationStats);
        for (ExpectedQuery expectedQuery : expectedQueries) {
            expectedQuery.insert(key, value);
        }
    }

    private void update(long key, long oldValue, long newValue) {
        bitmap.update(values(oldValue), values(newValue), key, Long.toString(key), operationStats);
        for (ExpectedQuery expectedQuery : expectedQueries) {
            expectedQuery.update(key, oldValue, newValue);
        }
    }

    private void remove(long key, long value) {
        bitmap.remove(values(value), key, operationStats);
        for (ExpectedQuery expectedQuery : expectedQueries) {
            expectedQuery.remove(key, value);
        }
    }

    private void clear() {
        bitmap.clear();
        for (ExpectedQuery expectedQuery : expectedQueries) {
            expectedQuery.clear();
        }
    }

    private void verify() {
        for (int i = 0; i < actualQueries.length; ++i) {
            Predicate actualQuery = actualQueries[i];
            ExpectedQuery expectedQuery = expectedQueries[i];

            Iterator<String> actualResult = bitmap.evaluate(actualQuery, INTEGER_CONVERTER);
            expectedQuery.verify(actualResult);
        }
    }

    private static Iterator<Integer> values(long key) {
        List<Integer> values = new ArrayList<>(Long.SIZE);
        for (int i = 0; i < Long.SIZE; ++i) {
            if ((key & 1L << i) != 0) {
                values.add(i);
            }
        }
        return values.iterator();
    }

    private static boolean bit(int bit, long value) {
        return (value & 1L << bit) != 0;
    }

    private interface LongPredicate {

        boolean test(long value);

    }

    private static class ExpectedQuery {

        private final LongPredicate predicate;
        private final SortedSet<Long> result = new TreeSet<>();

        ExpectedQuery(LongPredicate predicate) {
            this.predicate = predicate;
        }

        public void insert(long key, long value) {
            if (predicate.test(value)) {
                result.add(key);
            }
        }

        public void update(long key, long oldValue, long newValue) {
            // not really needed, just to harden the test
            if (predicate.test(oldValue)) {
                result.remove(key);
            }

            if (predicate.test(newValue)) {
                result.add(key);
            }
        }

        public void remove(long key, long value) {
            // not really needed, just to harden the test
            if (predicate.test(value)) {
                result.remove(key);
            }
        }

        public void clear() {
            result.clear();
        }

        public void verify(Iterator<String> actualResult) {
            Iterator<Long> expectedResult = result.iterator();
            while (actualResult.hasNext()) {
                long expected = expectedResult.next();
                long actual = Long.parseLong(actualResult.next());
                assertEquals(expected, actual);
            }
            assertFalse(expectedResult.hasNext());
        }

    }

}
