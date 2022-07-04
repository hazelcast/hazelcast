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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.Predicates.between;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.greaterEqual;
import static com.hazelcast.query.Predicates.greaterThan;
import static com.hazelcast.query.Predicates.in;
import static com.hazelcast.query.Predicates.lessEqual;
import static com.hazelcast.query.Predicates.lessThan;
import static com.hazelcast.query.Predicates.notEqual;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MixedTypeQueriesTest extends HazelcastTestSupport {

    private Random random;

    private IMap<Integer, Record> expected;
    private IMap<Integer, Record> actual;

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.OBJECT}, {InMemoryFormat.BINARY}});
    }

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Before
    public void before() {
        long seed = System.nanoTime();
        System.out.println("MixedTypeQueriesTest seed: " + seed);
        random = new Random(seed);

        Config config = getConfig();

        config.getMapConfig("expected").setInMemoryFormat(inMemoryFormat);
        config.getMapConfig("actual").setInMemoryFormat(inMemoryFormat);
        config.setProperty(QueryEngineImpl.DISABLE_MIGRATION_FALLBACK.getName(), "true");
        HazelcastInstance instance = createHazelcastInstance(config);

        expected = instance.getMap("expected");
        expected.addIndex(IndexType.HASH, "attr1", "attr2");
        expected.addIndex(IndexType.SORTED, "attr2", "attr1");
        expected.addIndex(IndexType.HASH, "attr3");
        expected.addIndex(IndexType.SORTED, "attr4");
        expected.addIndex(IndexType.SORTED, "attr2", "attr4");

        actual = instance.getMap("actual");
        actual.addIndex(IndexType.HASH, "attr1", "attr2");
        actual.addIndex(IndexType.SORTED, "attr2", "attr1");
        actual.addIndex(IndexType.HASH, "attr3");
        actual.addIndex(IndexType.SORTED, "attr4");
        actual.addIndex(IndexType.SORTED, "attr2", "attr4");

        for (int i = 0; i < 2000; ++i) {
            Number attr1 = randomNumber();
            Number attr2 = randomNumber();
            Number attr3 = randomNumber();
            Number attr4 = randomNumber();

            expected.put(i, new Record(attr1.doubleValue(), attr2.doubleValue(), attr3.doubleValue(), attr4.doubleValue()));
            actual.put(i, new Record(attr1, attr2, attr3, attr4));
        }
    }

    @Test
    public void testEqual() {
        for (int i = 0; i < 100; ++i) {
            Number number = randomNumber();
            double expected = number.doubleValue();
            Comparable actual;
            if (random.nextBoolean()) {
                actual = (Comparable) number;
            } else {
                actual = number.toString();
            }

            // no index
            assertPredicates(equal("attr1", expected), equal("attr1", actual));
            // first component of composite ordered index
            assertPredicates(equal("attr2", expected), equal("attr2", actual));
            // regular unordered index
            assertPredicates(equal("attr3", expected), equal("attr3", actual));
            // regular ordered index
            assertPredicates(equal("attr4", expected), equal("attr4", actual));

            // composite unordered index
            assertPredicates(and(equal("attr1", expected), equal("attr2", expected)),
                    and(equal("attr1", actual), equal("attr2", actual)));
            // composite ordered index
            assertPredicates(and(equal("attr2", expected), equal("attr4", expected)),
                    and(equal("attr2", actual), equal("attr4", actual)));
        }
    }

    @Test
    public void testNotEqual() {
        for (int i = 0; i < 100; ++i) {
            Number number = randomNumber();
            double expected = number.doubleValue();
            Comparable actual;
            if (random.nextBoolean()) {
                actual = (Comparable) number;
            } else {
                actual = number.toString();
            }

            // no index
            assertPredicates(notEqual("attr1", expected), notEqual("attr1", actual));

            // Indexes are not used by NotEqualPredicate in fact, the code below
            // is just for completeness.

            // first component of composite ordered index
            assertPredicates(notEqual("attr2", expected), notEqual("attr2", actual));
            // regular unordered index
            assertPredicates(notEqual("attr3", expected), notEqual("attr3", actual));
            // regular ordered index
            assertPredicates(notEqual("attr4", expected), notEqual("attr4", actual));

            // composite unordered index
            assertPredicates(and(notEqual("attr1", expected), notEqual("attr2", expected)),
                    and(notEqual("attr1", actual), notEqual("attr2", actual)));
            // composite ordered index
            assertPredicates(and(notEqual("attr2", expected), notEqual("attr4", expected)),
                    and(notEqual("attr2", actual), notEqual("attr4", actual)));
        }
    }

    @Test
    public void testIn() {
        for (int i = 0; i < 100; ++i) {
            Number number1 = randomNumber();
            double expected1 = number1.doubleValue();
            Comparable actual1;
            if (random.nextBoolean()) {
                actual1 = (Comparable) number1;
            } else {
                actual1 = number1.toString();
            }

            Number number2 = randomNumber();
            double expected2 = number2.doubleValue();
            Comparable actual2;
            if (random.nextBoolean()) {
                actual2 = (Comparable) number2;
            } else {
                actual2 = number2.toString();
            }

            // no index
            assertPredicates(in("attr1", expected1, expected2), in("attr1", actual1, actual2));
            // first component of composite ordered index
            assertPredicates(in("attr2", expected1, expected2), in("attr2", actual1, actual2));
            // regular unordered index
            assertPredicates(in("attr3", expected1, expected2), in("attr3", actual1, actual2));
            // regular ordered index
            assertPredicates(in("attr4", expected1, expected2), in("attr4", actual1, actual2));

            // InPredicate doesn't support composite indexes for now, the code
            // below is just for completeness.

            // composite unordered index
            assertPredicates(and(equal("attr1", expected1), in("attr2", expected1, expected2)),
                    and(equal("attr1", actual1), in("attr2", actual1, actual2)));
            // composite ordered index
            assertPredicates(and(equal("attr2", expected1), in("attr4", expected1, expected2)),
                    and(equal("attr2", actual1), in("attr4", actual1, actual2)));
        }
    }

    @Test
    public void testGreaterThan() {
        for (int i = 0; i < 100; ++i) {
            Number number = randomNumber();
            double expected = number.doubleValue();
            Comparable actual;
            if (random.nextBoolean()) {
                actual = (Comparable) number;
            } else {
                actual = number.toString();
            }

            // no index
            assertPredicates(greaterThan("attr1", expected), greaterThan("attr1", actual));
            // first component of composite ordered index
            assertPredicates(greaterThan("attr2", expected), greaterThan("attr2", actual));
            // regular unordered index
            assertPredicates(greaterThan("attr3", expected), greaterThan("attr3", actual));
            // regular ordered index
            assertPredicates(greaterThan("attr4", expected), greaterThan("attr4", actual));

            // composite unordered index, just for completeness
            assertPredicates(and(equal("attr1", expected), greaterThan("attr2", expected)),
                    and(equal("attr1", actual), greaterThan("attr2", actual)));
            // composite ordered index
            assertPredicates(and(equal("attr2", expected), greaterThan("attr4", expected)),
                    and(equal("attr2", actual), greaterThan("attr4", actual)));
        }
    }

    @Test
    public void testGreaterEqual() {
        for (int i = 0; i < 100; ++i) {
            Number number = randomNumber();
            double expected = number.doubleValue();
            Comparable actual;
            if (random.nextBoolean()) {
                actual = (Comparable) number;
            } else {
                actual = number.toString();
            }

            // no index
            assertPredicates(greaterEqual("attr1", expected), greaterEqual("attr1", actual));
            // first component of composite ordered index
            assertPredicates(greaterEqual("attr2", expected), greaterEqual("attr2", actual));
            // regular unordered index
            assertPredicates(greaterEqual("attr3", expected), greaterEqual("attr3", actual));
            // regular ordered index
            assertPredicates(greaterEqual("attr4", expected), greaterEqual("attr4", actual));

            // composite unordered index, just for completeness
            assertPredicates(and(equal("attr1", expected), greaterEqual("attr2", expected)),
                    and(equal("attr1", actual), greaterEqual("attr2", actual)));
            // composite ordered index
            assertPredicates(and(equal("attr2", expected), greaterEqual("attr4", expected)),
                    and(equal("attr2", actual), greaterEqual("attr4", actual)));
        }
    }

    @Test
    public void testLessThan() {
        for (int i = 0; i < 100; ++i) {
            Number number = randomNumber();
            double expected = number.doubleValue();
            Comparable actual;
            if (random.nextBoolean()) {
                actual = (Comparable) number;
            } else {
                actual = number.toString();
            }

            // no index
            assertPredicates(lessThan("attr1", expected), lessThan("attr1", actual));
            // first component of composite ordered index
            assertPredicates(lessThan("attr2", expected), lessThan("attr2", actual));
            // regular unordered index
            assertPredicates(lessThan("attr3", expected), lessThan("attr3", actual));
            // regular ordered index
            assertPredicates(lessThan("attr4", expected), lessThan("attr4", actual));

            // composite unordered index, just for completeness
            assertPredicates(and(equal("attr1", expected), lessThan("attr2", expected)),
                    and(equal("attr1", actual), lessThan("attr2", actual)));
            // composite ordered index
            assertPredicates(and(equal("attr2", expected), lessThan("attr4", expected)),
                    and(equal("attr2", actual), lessThan("attr4", actual)));
        }
    }

    @Test
    public void testLessEqual() {
        for (int i = 0; i < 100; ++i) {
            Number number = randomNumber();
            double expected = number.doubleValue();
            Comparable actual;
            if (random.nextBoolean()) {
                actual = (Comparable) number;
            } else {
                actual = number.toString();
            }

            // no index
            assertPredicates(lessEqual("attr1", expected), lessEqual("attr1", actual));
            // first component of composite ordered index
            assertPredicates(lessEqual("attr2", expected), lessEqual("attr2", actual));
            // regular unordered index
            assertPredicates(lessEqual("attr3", expected), lessEqual("attr3", actual));
            // regular ordered index
            assertPredicates(lessEqual("attr4", expected), lessEqual("attr4", actual));

            // composite unordered index, just for completeness
            assertPredicates(and(equal("attr1", expected), lessEqual("attr2", expected)),
                    and(equal("attr1", actual), lessEqual("attr2", actual)));
            // composite ordered index
            assertPredicates(and(equal("attr2", expected), lessEqual("attr4", expected)),
                    and(equal("attr2", actual), lessEqual("attr4", actual)));
        }
    }

    @Test
    public void testBetween() {
        for (int i = 0; i < 100; ++i) {
            Number from = randomNumber();
            double expectedFrom = from.doubleValue();
            Comparable actualFrom;
            if (random.nextBoolean()) {
                actualFrom = (Comparable) from;
            } else {
                actualFrom = from.toString();
            }

            Number to = randomNumber();
            double expectedTo = to.doubleValue();
            Comparable actualTo;
            if (random.nextBoolean()) {
                actualTo = (Comparable) to;
            } else {
                actualTo = to.toString();
            }

            // no index
            assertPredicates(between("attr1", expectedFrom, expectedTo), between("attr1", actualFrom, actualTo));
            // first component of composite ordered index
            assertPredicates(between("attr2", expectedFrom, expectedTo), between("attr2", actualFrom, actualTo));
            // regular unordered index
            assertPredicates(between("attr3", expectedFrom, expectedTo), between("attr3", actualFrom, actualTo));
            // regular ordered index
            assertPredicates(between("attr4", expectedFrom, expectedTo), between("attr4", actualFrom, actualTo));

            // composite unordered index, just for completeness
            assertPredicates(and(equal("attr1", expectedFrom), between("attr2", expectedFrom, expectedTo)),
                    and(equal("attr1", actualFrom), between("attr2", actualFrom, actualTo)));
            // composite ordered index
            assertPredicates(and(equal("attr2", expectedFrom), between("attr4", expectedFrom, expectedTo)),
                    and(equal("attr2", actualFrom), between("attr4", actualFrom, actualTo)));
        }
    }

    protected Config getConfig() {
        return smallInstanceConfig();
    }

    private Number randomNumber() {
        int type = random.nextInt(6);
        int value = random.nextInt(101) - 50;

        switch (type) {
            case 0:
                return random.nextBoolean() ? (double) value : value + 0.25D;
            case 1:
                return (long) value;
            case 2:
                return random.nextBoolean() ? (float) value : value + 0.25F;
            case 3:
                return value;
            case 4:
                return (short) value;
            case 5:
                return (byte) value;
            default:
                throw new IllegalStateException();
        }
    }

    private void assertPredicates(Predicate expectedPredicate, Predicate actualPredicate) {
        Set<Integer> expectedSet = expected.keySet(expectedPredicate);
        Set<Integer> actualSet = actual.keySet(actualPredicate);

        // The underlying result sets have contains performing in O(n), converting
        // to HashSet for performance.
        assertEquals(new HashSet<Integer>(expectedSet), new HashSet<Integer>(actualSet));
    }

    public static class Record implements Serializable {

        public final Object attr1;
        public final Object attr2;
        public final Object attr3;
        public final Object attr4;

        public Record(Object attr1, Object attr2, Object attr3, Object attr4) {
            this.attr1 = attr1;
            this.attr2 = attr2;
            this.attr3 = attr3;
            this.attr4 = attr4;
        }

    }

}
