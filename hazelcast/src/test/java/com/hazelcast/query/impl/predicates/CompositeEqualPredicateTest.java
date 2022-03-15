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
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.CompositeValue;
import com.hazelcast.query.impl.IndexUtils;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.ObjectTestUtils;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Random;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompositeEqualPredicateTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;
    private Random random;
    private IMap<Integer, Person> map;

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.OBJECT}, {InMemoryFormat.BINARY}});
    }

    @Before
    public void before() {
        long seed = System.currentTimeMillis();
        System.out.println("CompositeEqualPredicateTest seed: " + seed);
        random = new Random(seed);

        Config config = getConfig();
        config.getMapConfig("persons").setInMemoryFormat(inMemoryFormat);

        map = createHazelcastInstance(config).getMap("persons");

        for (int i = 0; i < 500; ++i) {
            map.put(i, new Person(randomAge(), randomHeight()));
        }
    }

    @Test
    public void testUnordered() {
        IndexConfig indexConfig = IndexUtils.createTestIndexConfig(IndexType.HASH, "age", "height");
        map.addIndex(indexConfig);
        assertEquals(0, map.getLocalMapStats().getIndexedQueryCount());

        for (int i = 0; i < 100; ++i) {
            final Integer age = randomQueryAge();
            final Long height = randomQueryHeight();

            assertPredicate(new Predicate<Integer, Person>() {
                @Override
                public boolean apply(Map.Entry<Integer, Person> mapEntry) {
                    return ObjectTestUtils.equals(mapEntry.getValue().age, age) && ObjectTestUtils.equals(
                            mapEntry.getValue().height, height);
                }
            }, predicate(indexConfig.getName(), value(age, height), "age", "height"));
        }

        assertEquals(100, map.getLocalMapStats().getIndexedQueryCount());
    }

    @Test
    public void testOrdered() {
        IndexConfig indexConfig = IndexUtils.createTestIndexConfig(IndexType.SORTED, "age", "height");

        map.addIndex(indexConfig);
        assertEquals(0, map.getLocalMapStats().getIndexedQueryCount());

        for (int i = 0; i < 100; ++i) {
            final Integer age = randomQueryAge();
            final Long height = randomQueryHeight();

            assertPredicate(new Predicate<Integer, Person>() {
                @Override
                public boolean apply(Map.Entry<Integer, Person> mapEntry) {
                    return ObjectTestUtils.equals(mapEntry.getValue().age, age) && ObjectTestUtils.equals(
                            mapEntry.getValue().height, height);
                }
            }, predicate(indexConfig.getName(), value(age, height), "age", "height"));
        }

        assertEquals(100, map.getLocalMapStats().getIndexedQueryCount());
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    private void assertPredicate(Predicate expected, Predicate actual) {
        assertEquals(actual.toString(), map.entrySet(expected), map.entrySet(actual));
        assertEquals(actual.toString(), map.entrySet(expected), map.entrySet(new SkipIndexPredicate(actual)));
    }

    private Integer randomAge() {
        int value = random.nextInt(50);
        return value == 0 ? null : value;
    }

    private Integer randomQueryAge() {
        int value = random.nextInt(60) - 5;
        return value == 0 ? null : value;
    }

    private Long randomHeight() {
        long value = random.nextInt(50);
        return value == 0 ? null : value;
    }

    private Long randomQueryHeight() {
        long value = random.nextInt(60) - 5;
        return value == 0 ? null : value;
    }

    private static Predicate predicate(String indexName, CompositeValue value, String... components) {
        return new CompositeEqualPredicate(indexName, components, value);
    }

    private static CompositeValue value(Comparable... values) {
        return new CompositeValue(values);
    }

    public static class Person implements Serializable {

        public final Integer age;
        public final Long height;

        public Person(Integer age, Long height) {
            this.age = age;
            this.height = height;
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

            if (age != null ? !age.equals(person.age) : person.age != null) {
                return false;
            }
            return height != null ? height.equals(person.height) : person.height == null;
        }

        @Override
        public String toString() {
            return "Person{" + "age=" + age + ", height=" + height + '}';
        }

    }

}
