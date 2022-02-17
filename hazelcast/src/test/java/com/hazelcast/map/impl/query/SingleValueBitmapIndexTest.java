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

import com.hazelcast.config.BitmapIndexOptions.UniqueKeyTransformation;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.Partition;
import com.hazelcast.query.LocalIndexStats;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.IntPredicate;

import static com.hazelcast.config.BitmapIndexOptions.UniqueKeyTransformation.LONG;
import static com.hazelcast.config.BitmapIndexOptions.UniqueKeyTransformation.OBJECT;
import static com.hazelcast.config.BitmapIndexOptions.UniqueKeyTransformation.RAW;
import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.in;
import static com.hazelcast.query.Predicates.notEqual;
import static com.hazelcast.query.Predicates.or;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SingleValueBitmapIndexTest extends HazelcastTestSupport {

    private static final int BATCH_SIZE = 1000;
    private static final int BATCH_COUNT = 10;

    @Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        // @formatter:off
        return asList(new Object[][]{
                {makeConfig(null, null)},
                {makeConfig("stringId", null)},
                {makeConfig("stringId", OBJECT)},
                {makeConfig(null, RAW)},
                {makeConfig(null, LONG)}
        });
        // @formatter:on
    }

    private static final Predicate[] actualQueries;

    static {
        actualQueries = new Predicate[8];
        actualQueries[0] = equal("age", new Age(0));
        actualQueries[1] = equal("age", null);
        actualQueries[2] = notEqual("age", null);
        actualQueries[3] = notEqual("age", new Age(1));
        actualQueries[4] = equal("age", new Age(50));
        actualQueries[5] = and(equal("age", new Age(50)), notEqual("age", new Age(99)));
        actualQueries[6] = or(equal("age", new Age(50)), equal("age", new Age(99)));
        actualQueries[7] = or(equal("age", new Age(5)), in("age", new Age(10), null));
    }

    private final ExpectedQuery[] expectedQueries;

    {
        expectedQueries = new ExpectedQuery[8];
        expectedQueries[0] = new ExpectedQuery(value -> {
            // no zero values at all, they are all nulls
            return false;
        });
        expectedQueries[1] = new ExpectedQuery(value -> value == 0);
        expectedQueries[2] = new ExpectedQuery(value -> value != 0);
        expectedQueries[3] = new ExpectedQuery(value -> value != 1);
        expectedQueries[4] = new ExpectedQuery(value -> value == 50);
        //noinspection ExcessiveRangeCheck,ConstantConditions
        expectedQueries[5] = new ExpectedQuery(value -> value == 50 && value != 99);
        expectedQueries[6] = new ExpectedQuery(value -> value == 50 || value == 99);
        expectedQueries[7] = new ExpectedQuery(value -> value == 5 || value == 10 || value == 0);
    }

    @Rule
    public TestName testName = new TestName();

    @Parameter
    public IndexConfig indexConfig;

    private IMap<Long, Person> persons;

    private IMap<Long, Person> personsA;
    private IMap<Long, Person> personsB;

    private TestHazelcastInstanceFactory factory;

    @Before
    public void before() {
        factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instanceA = factory.newHazelcastInstance(getConfig());
        HazelcastInstance instanceB = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(instanceA, instanceB);

        persons = instanceA.getMap("persons");

        personsA = persons;
        personsB = instanceB.getMap("persons");
    }

    @Test
    public void testConsecutiveQueries() {
        for (int i = BATCH_COUNT - 1; i >= 0; --i) {
            for (long j = 0; j < BATCH_SIZE; ++j) {
                long id = i * BATCH_SIZE + j;
                put(id, (int) id);
            }
            verifyQueries();
        }

        for (int i = 0; i < BATCH_COUNT; ++i) {
            for (long j = 0; j < BATCH_SIZE; ++j) {
                long id = i * BATCH_SIZE + j;
                if (i % 2 == 0) {
                    put(id, (int) id + 1);
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
                put(id, (int) id);
            }
            verifyQueries();
        }

        random = new Random(seed);
        for (int i = 0; i < BATCH_COUNT; ++i) {
            for (int j = 0; j < BATCH_SIZE; ++j) {
                long id = random.nextInt(i * BATCH_SIZE + j + 1);
                if (i % 2 == 0) {
                    put(id, (int) id + 1);
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
    public void testClearedIndexes() {
        // Populate the index and run queries as usual.

        for (int i = BATCH_COUNT - 1; i >= 0; --i) {
            for (long j = 0; j < BATCH_SIZE; ++j) {
                long id = i * BATCH_SIZE + j;
                put(id, (int) id);
            }
            verifyQueries();
        }

        // Clear the index leaving the map populated and marking the index as
        // valid for queries.

        for (HazelcastInstance instance : factory.getAllHazelcastInstances()) {
            HazelcastInstanceImpl instanceImpl = (HazelcastInstanceImpl) instance;
            MapService mapService = instanceImpl.node.getNodeEngine().getService(MapService.SERVICE_NAME);

            Indexes indexes = mapService.getMapServiceContext().getMapContainer(persons.getName()).getIndexes();
            indexes.clearAll();

            for (Partition partition : instanceImpl.getPartitionService().getPartitions()) {
                if (partition.getOwner().localMember()) {
                    Indexes.beginPartitionUpdate(indexes.getIndexes());
                    Indexes.markPartitionAsIndexed(partition.getPartitionId(), indexes.getIndexes());
                }
            }
        }

        // Clear the expected results: we are repopulating indexes.

        for (ExpectedQuery expectedQuery : expectedQueries) {
            expectedQuery.clear();
        }

        // Repopulate the index and run queries. Technically, we are doing index
        // updates here instead of inserts since the map is still populated, but
        // the index interprets them as inserts.

        persons.getLocalMapStats().getIndexStats();

        for (int i = BATCH_COUNT - 1; i >= 0; --i) {
            for (long j = 0; j < BATCH_SIZE; ++j) {
                long id = i * BATCH_SIZE + j;
                put(id, (int) id);
            }
            verifyQueries();
        }

        LocalIndexStats statsA = personsA.getLocalMapStats().getIndexStats().values().iterator().next();
        LocalIndexStats statsB = personsB.getLocalMapStats().getIndexStats().values().iterator().next();
        assertEquals(BATCH_COUNT * BATCH_SIZE, statsA.getInsertCount() + statsB.getInsertCount());
        assertEquals(BATCH_COUNT * BATCH_SIZE, statsA.getUpdateCount() + statsB.getUpdateCount());
    }

    @Override
    protected Config getConfig() {
        Config config = HazelcastTestSupport.smallInstanceConfig();
        config.setProperty(QueryEngineImpl.DISABLE_MIGRATION_FALLBACK.getName(), "true");
        MapConfig mapConfig = config.getMapConfig("persons");
        mapConfig.addIndexConfig(indexConfig);
        // disable periodic metrics collection (may interfere with the test)
        config.getMetricsConfig().setEnabled(false);
        return config;
    }

    private void put(long id, int age) {
        age = age % 100;
        Person person = new Person(id, age == 0 ? null : age);
        persons.put(id, person);
        for (ExpectedQuery expectedQuery : expectedQueries) {
            expectedQuery.put(id, person, age);
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

    @SuppressWarnings("unchecked")
    private void verifyQueries() {
        for (int i = 0; i < actualQueries.length; ++i) {
            Predicate actualQuery = actualQueries[i];
            ExpectedQuery expectedQuery = expectedQueries[i];

            long before = persons.getLocalMapStats().getIndexStats().values().iterator().next().getQueryCount();
            Set<Map.Entry<Long, Person>> entries = persons.entrySet(actualQuery);
            long after = persons.getLocalMapStats().getIndexStats().values().iterator().next().getQueryCount();
            assertEquals(1, after - before);
            expectedQuery.verify(entries);
        }
    }

    public static class Person implements DataSerializable {

        public long id;

        public Age age;

        public String stringId;

        public Person(long id, Integer age) {
            this.id = id;
            this.age = age == null ? null : new Age(age);
            this.stringId = Long.toString(id);
        }

        public Person() {
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
            if (!Objects.equals(age, person.age)) {
                return false;
            }
            return stringId.equals(person.stringId);
        }

        @Override
        public int hashCode() {
            int result = (int) (id ^ (id >>> 32));
            result = 31 * result + (age != null ? age.hashCode() : 0);
            result = 31 * result + stringId.hashCode();
            return result;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(id);
            out.writeString(stringId);
            out.writeObject(age);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            id = in.readLong();
            stringId = in.readString();
            age = in.readObject();
        }

        @Override
        public String toString() {
            return "Person{id=" + id + "}";
        }

    }

    private static class ExpectedQuery {

        private final IntPredicate predicate;
        private final Map<Long, Person> result = new HashMap<>();

        ExpectedQuery(IntPredicate predicate) {
            this.predicate = predicate;
        }

        public void put(long id, Person person, int age) {
            result.remove(id);
            if (predicate.test(age)) {
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

    public static class Age implements Serializable, Comparable<Age> {

        private final int age;

        public Age(int age) {
            this.age = age;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Age that = (Age) o;

            return age == that.age;
        }

        @Override
        public int hashCode() {
            return age;
        }

        @Override
        public String toString() {
            return "Age{" + "age=" + age + '}';
        }

        @Override
        public int compareTo(Age o) {
            return age - o.age;
        }

    }

    private static IndexConfig makeConfig(String uniqueKey, UniqueKeyTransformation uniqueKeyTransformation) {
        IndexConfig config = new IndexConfig(IndexType.BITMAP, "age");
        if (uniqueKey != null) {
            config.getBitmapIndexOptions().setUniqueKey(uniqueKey);
        }
        if (uniqueKeyTransformation != null) {
            config.getBitmapIndexOptions().setUniqueKeyTransformation(uniqueKeyTransformation);
        }
        return config;
    }

}
