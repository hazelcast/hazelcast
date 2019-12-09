/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.in;
import static com.hazelcast.query.Predicates.not;
import static com.hazelcast.query.Predicates.notEqual;
import static com.hazelcast.query.Predicates.or;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class MultiValueBitmapIndexTest extends HazelcastTestSupport {

    private static final int BATCH_SIZE = 1000;
    private static final int BATCH_COUNT = 10;

    @Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        // @formatter:off
        return asList(new Object[][]{
                {"BITMAP(habits[any])"},
                {"BITMAP(habits[any], stringId)"},
                {"BITMAP(habits[any], stringId, OBJECT)"},
                {"BITMAP(habits[any], __key, RAW)"},
                {"BITMAP(habits[any], __key, LONG)"}
        });
        // @formatter:on
    }

    private static final Predicate[] actualQueries;

    static {
        actualQueries = new Predicate[9];
        actualQueries[0] = notEqual("habits[any]", "0");
        actualQueries[1] = equal("habits[any]", 1L);
        actualQueries[2] = equal("habits[any]", 2);
        actualQueries[3] = or(equal("habits[any]", 1L), equal("habits[any]", 2));
        actualQueries[4] = in("habits[any]", 3, 4);
        actualQueries[5] = not(in("habits[any]", 3, "4"));

        // all together
        actualQueries[6] = and(equal("habits[any]", 1L), or(notEqual("habits[any]", "0"), equal("habits[any]", 2)),
                not(in("habits[any]", 3, "4")));

        // negate the previous one
        actualQueries[7] = not(and(equal("habits[any]", 1L), or(notEqual("habits[any]", "0"), equal("habits[any]", 2)),
                not(in("habits[any]", 3, "4"))));

        // try really dense query (returns all entries)
        actualQueries[8] = equal("habits[any]", "0");
    }

    private final ExpectedQuery[] expectedQueries;

    {
        expectedQueries = new ExpectedQuery[9];
        expectedQueries[0] = new ExpectedQuery(new LongPredicate() {
            @Override
            public boolean test(long value) {
                return !bit(0, value);
            }
        });
        expectedQueries[1] = new ExpectedQuery(new LongPredicate() {
            @Override
            public boolean test(long value) {
                return bit(1, value);
            }
        });
        expectedQueries[2] = new ExpectedQuery(new LongPredicate() {
            @Override
            public boolean test(long value) {
                return bit(2, value);
            }
        });
        expectedQueries[3] = new ExpectedQuery(new LongPredicate() {
            @Override
            public boolean test(long value) {
                return bit(1, value) || bit(2, value);
            }
        });
        expectedQueries[4] = new ExpectedQuery(new LongPredicate() {
            @Override
            public boolean test(long value) {
                return bit(3, value) || bit(4, value);
            }
        });
        expectedQueries[5] = new ExpectedQuery(new LongPredicate() {
            @Override
            public boolean test(long value) {
                return !(bit(3, value) || bit(4, value));
            }
        });
        expectedQueries[6] = new ExpectedQuery(new LongPredicate() {
            @Override
            public boolean test(long value) {
                return bit(1, value) && (!bit(0, value) || bit(2, value)) && !(bit(3, value) || bit(4, value));
            }
        });
        expectedQueries[7] = new ExpectedQuery(new LongPredicate() {
            @Override
            public boolean test(long value) {
                return !(bit(1, value) && (!bit(0, value) || bit(2, value)) && !(bit(3, value) || bit(4, value)));
            }
        });
        expectedQueries[8] = new ExpectedQuery(new LongPredicate() {
            @Override
            public boolean test(long value) {
                return bit(0, value);
            }
        });
    }

    @Rule
    public TestName testName = new TestName();

    @Parameter
    public String indexDefinition;

    private IMap<Long, Person> persons;

    @Before
    public void before() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());
        persons = instance.getMap("persons");
        factory.newHazelcastInstance(getConfig());
    }

    @Test
    public void testConsecutiveQueries() {
        for (int i = BATCH_COUNT - 1; i >= 0; --i) {
            for (long j = 0; j < BATCH_SIZE; ++j) {
                long id = i * BATCH_SIZE + j;
                put(id, habits(id));
            }
            verifyQueries();
        }

        for (int i = 0; i < BATCH_COUNT; ++i) {
            for (long j = 0; j < BATCH_SIZE; ++j) {
                long id = i * BATCH_SIZE + j;
                if (i % 2 == 0) {
                    put(id, habits(id + 1));
                } else {
                    remove(id);
                }
            }
            verifyQueries();
        }

        clear();
        verifyQueries();
    }

    @Test
    public void testRandomQueries() {
        long seed = System.nanoTime();
        System.out.println(testName.getMethodName() + " seed: " + seed);
        Random random = new Random(seed);

        for (int i = 0; i < BATCH_COUNT; ++i) {
            for (int j = 0; j < BATCH_SIZE; ++j) {
                long id = random.nextInt(i * BATCH_SIZE + j + 1);
                put(id, habits(id));
            }
            verifyQueries();
        }

        random = new Random(seed);
        for (int i = 0; i < BATCH_COUNT; ++i) {
            for (int j = 0; j < BATCH_SIZE; ++j) {
                long id = random.nextInt(i * BATCH_SIZE + j + 1);
                if (i % 2 == 0) {
                    put(id, habits(id + 1));
                } else {
                    remove(id);
                }
            }
            verifyQueries();
        }

        clear();
        verifyQueries();
    }

    @Override
    protected Config getConfig() {
        Config config = HazelcastTestSupport.smallInstanceConfig();
        MapConfig mapConfig = config.getMapConfig("persons");
        mapConfig.addMapIndexConfig(new MapIndexConfig(indexDefinition, false));
        return config;
    }

    private static long habits(long id) {
        return id << 1 | 1;
    }

    private void put(long id, long habits) {
        Person person = new Person(id, habits);
        persons.put(id, person);
        for (ExpectedQuery expectedQuery : expectedQueries) {
            expectedQuery.put(id, person, habits);
        }
    }

    private void remove(long id) {
        persons.remove(id);
        for (ExpectedQuery expectedQuery : expectedQueries) {
            expectedQuery.remove(id);
        }
    }

    private void clear() {
        persons.clear();
        for (ExpectedQuery expectedQuery : expectedQueries) {
            expectedQuery.clear();
        }
    }

    private void verifyQueries() {
        for (int i = 0; i < actualQueries.length; ++i) {
            Predicate actualQuery = actualQueries[i];
            ExpectedQuery expectedQuery = expectedQueries[i];

            Set<Map.Entry<Long, Person>> entries = persons.entrySet(actualQuery);
            expectedQuery.verify(entries);
        }
    }

    public static class Person implements DataSerializable {

        public long id;

        public long[] habits;

        public String stringId;

        public Person(long id, long habitsLong) {
            this.id = id;
            this.stringId = Long.toString(id);

            long[] habits = new long[Long.bitCount(habitsLong)];
            int count = 0;
            for (int i = 0; i < Long.SIZE; ++i) {
                if ((habitsLong & 1L << i) != 0) {
                    habits[count] = i;
                    ++count;
                }
            }
            assert count == Long.bitCount(habitsLong);
            this.habits = habits;
        }

        public Person() {
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(id);
            out.writeUTF(stringId);
            out.writeLongArray(habits);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            id = in.readLong();
            stringId = in.readUTF();
            habits = in.readLongArray();
        }

        @Override
        public String toString() {
            return "Person{id=" + id + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Person person = (Person) o;

            if (id != person.id) {
                return false;
            }
            if (!Arrays.equals(habits, person.habits)) {
                return false;
            }
            return stringId.equals(person.stringId);
        }

        @Override
        public int hashCode() {
            int result = (int) (id ^ (id >>> 32));
            result = 31 * result + Arrays.hashCode(habits);
            result = 31 * result + stringId.hashCode();
            return result;
        }

    }

    private static boolean bit(int bit, long value) {
        return (value & 1L << bit) != 0;
    }

    private interface LongPredicate {

        boolean test(long value);

    }

    private static class ExpectedQuery {

        private final LongPredicate predicate;
        private final Map<Long, Person> result = new HashMap<Long, Person>();

        ExpectedQuery(LongPredicate predicate) {
            this.predicate = predicate;
        }

        public void put(long id, Person person, long habits) {
            result.remove(id);
            if (predicate.test(habits)) {
                result.put(id, person);
            }
        }

        public void remove(long id) {
            result.remove(id);
        }

        public void verify(Set<Map.Entry<Long, Person>> actual) {
            assertEquals(result.size(), actual.size());
            for (Map.Entry<Long, Person> actualEntry : actual) {
                Person person = result.get(actualEntry.getKey());
                assertEquals(person, actualEntry.getValue());
            }
        }

        public void clear() {
            result.clear();
        }

    }

}
