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

import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static com.hazelcast.query.impl.CompositeValue.NEGATIVE_INFINITY;
import static com.hazelcast.query.impl.CompositeValue.POSITIVE_INFINITY;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompositeRangePredicateTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;
    private Random random;
    private IMap<Integer, Person> map;
    private String indexName;

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.OBJECT}, {InMemoryFormat.BINARY}});
    }

    @Before
    public void before() {
        long seed = System.currentTimeMillis();
        System.out.println("CompositeRangePredicateTest seed: " + seed);
        random = new Random(seed);

        Config config = getConfig();
        config.getMapConfig("persons").setInMemoryFormat(inMemoryFormat);

        map = createHazelcastInstance(config).getMap("persons");

        IndexConfig indexConfig = IndexUtils.createTestIndexConfig(IndexType.SORTED, "age", "height", "__key");

        indexName = indexConfig.getName();

        map.addIndex(indexConfig);

        for (int i = 0; i < 500; ++i) {
            map.put(i, new Person(randomAge(), randomHeight()));
        }
    }

    @Test
    public void testNoComparison() {
        assertEquals(0, map.getLocalMapStats().getIndexedQueryCount());

        for (int i = 0; i < 100; ++i) {
            final Integer age = randomQueryAge();
            final Long height = randomQueryHeight(true);

            int prefixLength = random.nextInt(2) + 1;

            final CompositeValue from;
            final CompositeValue to;
            final Predicate expected;

            switch (prefixLength) {
                case 1:
                    from = value(age, NEGATIVE_INFINITY, NEGATIVE_INFINITY);
                    to = value(age, POSITIVE_INFINITY, POSITIVE_INFINITY);
                    expected = new Predicate<Integer, Person>() {
                        @Override
                        public boolean apply(Map.Entry<Integer, Person> mapEntry) {
                            return ObjectTestUtils.equals(mapEntry.getValue().age, age);
                        }
                    };
                    break;
                case 2:
                    from = value(age, height, NEGATIVE_INFINITY);
                    to = value(age, height, POSITIVE_INFINITY);
                    expected = new Predicate<Integer, Person>() {
                        @Override
                        public boolean apply(Map.Entry<Integer, Person> mapEntry) {
                            return ObjectTestUtils.equals(mapEntry.getValue().age, age) && ObjectTestUtils.equals(
                                    mapEntry.getValue().height, height);
                        }
                    };
                    break;
                default:
                    throw new IllegalStateException();
            }

            assertPredicate(
                expected,
                predicate(indexName, from, false, to, false, prefixLength, "age", "height", "__key")
            );
        }

        assertEquals(100, map.getLocalMapStats().getIndexedQueryCount());
    }

    @Test
    public void testComparison() {
        assertEquals(0, map.getLocalMapStats().getIndexedQueryCount());

        for (int i = 0; i < 100; ++i) {
            final Integer age = randomQueryAge();
            final Long height = randomQueryHeight(true);

            int prefixLength = random.nextInt(2) + 1;

            final CompositeValue from;
            final boolean fromInclusive;
            final CompositeValue to;
            final boolean toInclusive;
            final Predicate expected;

            switch (prefixLength) {
                case 1:
                    final Long heightFrom = randomQueryHeight(true);
                    final boolean heightFromInclusive = heightFrom != null && random.nextBoolean();
                    final Long heightTo = randomQueryHeight(heightFrom != null);
                    final boolean heightToInclusive = heightTo != null && random.nextBoolean();

                    from = value(age, heightFrom != null ? heightFrom : NULL,
                            heightFromInclusive ? NEGATIVE_INFINITY : POSITIVE_INFINITY);
                    fromInclusive = false;
                    to = value(age, heightTo != null ? heightTo : POSITIVE_INFINITY,
                            heightToInclusive ? POSITIVE_INFINITY : NEGATIVE_INFINITY);
                    toInclusive = false;

                    expected = new Predicate<Integer, Person>() {
                        @SuppressWarnings("RedundantIfStatement")
                        @Override
                        public boolean apply(Map.Entry<Integer, Person> mapEntry) {
                            Person value = mapEntry.getValue();

                            if (!ObjectTestUtils.equals(value.age, age)) {
                                return false;
                            }

                            if (value.height == null) {
                                return false;
                            }

                            if (heightFrom != null) {
                                if (heightFromInclusive) {
                                    if (value.height < heightFrom) {
                                        return false;
                                    }
                                } else {
                                    if (value.height <= heightFrom) {
                                        return false;
                                    }
                                }
                            }

                            if (heightTo != null) {
                                if (heightToInclusive) {
                                    if (value.height > heightTo) {
                                        return false;
                                    }
                                } else {
                                    if (value.height >= heightTo) {
                                        return false;
                                    }
                                }
                            }

                            return true;
                        }
                    };
                    break;
                case 2:
                    final Integer keyFrom = randomQueryKey(true);
                    final boolean keyFromInclusive = keyFrom != null && random.nextBoolean();
                    final Integer keyTo = randomQueryKey(keyFrom != null);
                    final boolean keyToInclusive = keyTo != null && random.nextBoolean();

                    from = value(age, height, keyFrom != null ? keyFrom : NULL);
                    fromInclusive = keyFromInclusive;
                    to = value(age, height, keyTo != null ? keyTo : POSITIVE_INFINITY);
                    toInclusive = keyToInclusive;

                    expected = new Predicate<Integer, Person>() {
                        @SuppressWarnings("RedundantIfStatement")
                        @Override
                        public boolean apply(Map.Entry<Integer, Person> mapEntry) {
                            Person value = mapEntry.getValue();
                            int key = mapEntry.getKey();

                            if (!ObjectTestUtils.equals(value.age, age)) {
                                return false;
                            }

                            if (!ObjectTestUtils.equals(value.height, height)) {
                                return false;
                            }

                            if (keyFrom != null) {
                                if (keyFromInclusive) {
                                    if (key < keyFrom) {
                                        return false;
                                    }
                                } else {
                                    if (key <= keyFrom) {
                                        return false;
                                    }
                                }
                            }

                            if (keyTo != null) {
                                if (keyToInclusive) {
                                    if (key > keyTo) {
                                        return false;
                                    }
                                } else {
                                    if (key >= keyTo) {
                                        return false;
                                    }
                                }
                            }

                            return true;
                        }
                    };
                    break;
                default:
                    throw new IllegalStateException();
            }

            assertPredicate(
                expected,
                predicate(indexName, from, fromInclusive, to, toInclusive, prefixLength, "age", "height", "__key")
            );
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

    private Long randomQueryHeight(boolean allowNull) {
        long value = random.nextInt(60) - 5;
        return value == 0 && allowNull ? null : value;
    }

    private Integer randomQueryKey(boolean allowNull) {
        int value = random.nextInt(510) - 5;
        return value == 0 && allowNull ? null : value;
    }

    private static Predicate predicate(String indexName, CompositeValue from, boolean fromInclusive, CompositeValue to,
        boolean toInclusive, int prefixLength, String... components) {
        return new CompositeRangePredicate(
            indexName,
            components,
            from,
            fromInclusive,
            to,
            toInclusive,
            prefixLength
        );
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
