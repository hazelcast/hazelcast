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

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexType;
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
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.query.Predicates.alwaysFalse;
import static com.hazelcast.query.Predicates.not;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BoundedRangePredicateQueriesTest extends HazelcastTestSupport {

    private static final int MIN = -100;
    private static final int MAX = +100;
    // +1 for inclusive +100 bound and one more +2 for nulls
    private static final int TOTAL = MAX - MIN + 3;

    @Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.OBJECT}, {InMemoryFormat.BINARY}});
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    private IMap<Integer, Person> map;

    @Before
    public void before() {
        Config config = getConfig();
        config.getMapConfig("persons").setInMemoryFormat(inMemoryFormat);

        map = createHazelcastInstance(config).getMap("persons");
        map.addIndex(IndexType.HASH, "age");
        map.addIndex(IndexType.SORTED, "height");
        // the weight attribute is unindexed

        map.put(MIN - 1, new Person(null));
        for (int i = MIN; i <= MAX; ++i) {
            map.put(i, new Person(i));
        }
        map.put(MAX + 1, new Person(null));
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void testClosedRange() {
        Predicate expected = new PersonPredicate(new java.util.function.Predicate<Integer>() {
            @Override
            public boolean test(Integer value) {
                return value != null && value >= 1 && value <= 10;
            }
        });

        assertPredicate(expected, new BoundedRangePredicate("age", 1, true, 10, true));
        assertPredicate(expected, new BoundedRangePredicate("height", 1, true, 10, true));
        assertPredicate(expected, new BoundedRangePredicate("weight", 1, true, 10, true));
    }

    @Test
    public void testLeftOpenRange() {
        Predicate expected = new PersonPredicate(new java.util.function.Predicate<Integer>() {
            @Override
            public boolean test(Integer value) {
                return value != null && value > 1 && value <= 10;
            }
        });

        assertPredicate(expected, new BoundedRangePredicate("age", 1, false, 10, true));
        assertPredicate(expected, new BoundedRangePredicate("height", 1, false, 10, true));
        assertPredicate(expected, new BoundedRangePredicate("weight", 1, false, 10, true));
    }

    @Test
    public void testRightOpenRange() {
        Predicate expected = new PersonPredicate(new java.util.function.Predicate<Integer>() {
            @Override
            public boolean test(Integer value) {
                return value != null && value >= 1 && value < 10;
            }
        });

        assertPredicate(expected, new BoundedRangePredicate("age", 1, true, 10, false));
        assertPredicate(expected, new BoundedRangePredicate("height", 1, true, 10, false));
        assertPredicate(expected, new BoundedRangePredicate("weight", 1, true, 10, false));
    }

    @Test
    public void testOpenRange() {
        Predicate expected = new PersonPredicate(new java.util.function.Predicate<Integer>() {
            @Override
            public boolean test(Integer value) {
                return value != null && value > 1 && value < 10;
            }
        });

        assertPredicate(expected, new BoundedRangePredicate("age", 1, false, 10, false));
        assertPredicate(expected, new BoundedRangePredicate("height", 1, false, 10, false));
        assertPredicate(expected, new BoundedRangePredicate("weight", 1, false, 10, false));
    }

    @Test
    public void testDegenerateRange() {
        Predicate expected = new PersonPredicate(new java.util.function.Predicate<Integer>() {
            @Override
            public boolean test(Integer value) {
                return value != null && value >= 1 && value <= 1;
            }
        });

        assertPredicate(expected, new BoundedRangePredicate("age", 1, true, 1, true));
        assertPredicate(expected, new BoundedRangePredicate("height", 1, true, 1, true));
        assertPredicate(expected, new BoundedRangePredicate("weight", 1, true, 1, true));
    }

    @Test
    public void testEmptyRanges() {
        assertPredicate(alwaysFalse(), new BoundedRangePredicate("age", 1, true, 1, false));
        assertPredicate(alwaysFalse(), new BoundedRangePredicate("height", 1, true, 1, false));
        assertPredicate(alwaysFalse(), new BoundedRangePredicate("weight", 1, true, 1, false));

        assertPredicate(alwaysFalse(), new BoundedRangePredicate("age", 1, false, 1, true));
        assertPredicate(alwaysFalse(), new BoundedRangePredicate("height", 1, false, 1, true));
        assertPredicate(alwaysFalse(), new BoundedRangePredicate("weight", 1, false, 1, true));

        assertPredicate(alwaysFalse(), new BoundedRangePredicate("age", 1, false, 1, false));
        assertPredicate(alwaysFalse(), new BoundedRangePredicate("height", 1, false, 1, false));
        assertPredicate(alwaysFalse(), new BoundedRangePredicate("weight", 1, false, 1, false));

        assertPredicate(alwaysFalse(), new BoundedRangePredicate("age", 1, true, 0, true));
        assertPredicate(alwaysFalse(), new BoundedRangePredicate("height", 1, true, 0, true));
        assertPredicate(alwaysFalse(), new BoundedRangePredicate("weight", 1, true, 0, true));

        assertPredicate(alwaysFalse(), new BoundedRangePredicate("age", +10, false, -10, false));
        assertPredicate(alwaysFalse(), new BoundedRangePredicate("height", +10, false, -10, false));
        assertPredicate(alwaysFalse(), new BoundedRangePredicate("weight", +10, false, -10, false));
    }

    @SuppressWarnings("unchecked")
    private void assertPredicate(Predicate expected, Predicate actual) {
        Set<Map.Entry<Integer, Person>> result = map.entrySet(actual);
        for (Map.Entry<Integer, Person> entry : result) {
            assertTrue(expected.apply(entry));
        }

        Set<Map.Entry<Integer, Person>> notResult = map.entrySet(not(actual));
        for (Map.Entry<Integer, Person> entry : notResult) {
            assertFalse(expected.apply(entry));
        }

        assertEquals(TOTAL, result.size() + notResult.size());
    }

    private static class PersonPredicate implements Predicate<Integer, Person> {

        private final java.util.function.Predicate<Integer> predicate;

        PersonPredicate(java.util.function.Predicate<Integer> predicate) {
            this.predicate = predicate;
        }

        @Override
        public boolean apply(Map.Entry<Integer, Person> entry) {
            Person person = entry.getValue();
            return predicate.test(person.age) && predicate.test(person.height) && predicate.test(person.weight);
        }

    }

    public static class Person implements Serializable {

        public final Integer age;
        public final Integer height;
        public final Integer weight;

        public Person(Integer value) {
            this.age = value;
            this.height = value;
            this.weight = value;
        }

    }

}
