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

package com.hazelcast.query.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.predicates.SqlPredicate;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.config.IndexType.BITMAP;
import static com.hazelcast.config.IndexType.SORTED;
import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.Predicates.between;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.greaterEqual;
import static com.hazelcast.query.Predicates.greaterThan;
import static com.hazelcast.query.Predicates.in;
import static com.hazelcast.query.Predicates.lessEqual;
import static com.hazelcast.query.Predicates.lessThan;
import static com.hazelcast.query.Predicates.like;
import static com.hazelcast.query.Predicates.or;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexConcurrencyTest extends AbstractIndexConcurrencyTest {

    @Parameterized.Parameters(name = "indexAttribute: {0}, indexType: {1}")
    public static Collection<Object[]> parameters() {
        // @formatter:off
        return asList(new Object[][]{
                {"age", SORTED},
                {"age", BITMAP},
                {"name", SORTED}
        });
        // @formatter:on
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void testIndexCreationAndQueryDeterministicConcurrency() {
        Config config = getConfig();

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Person> map = node.getMap(randomMapName());

        // put some data
        for (int i = 0; i < 10000; ++i) {
            map.put(i, new Person(i));
        }

        // initialize age field access counter
        Person.accessCountDown = new AtomicLong(5000);

        // start indexer, it will await for latch in the middle
        AtomicReference<Throwable> exception = new AtomicReference<>();
        Thread indexer = new Thread(() -> {
            try {
                map.addIndex(indexType, indexAttribute);
            } catch (Throwable t) {
                exception.compareAndSet(null, t);
            }
        });
        indexer.start();

        // await for query latch
        assertOpenEventually(Person.queryLatch);
        Person.accessCountDown = null;

        // run checking query
        assertQuery(map, new SqlPredicate("age >= 5000"), 5000);
        assertQuery(map, equal("age", "6000"), 1);
        assertQuery(map, greaterThan("age", "6000"), 3999);
        assertQuery(map, and(greaterThan("age", 6000), lessThan("age", 7000)), 999);
        assertQuery(map, or(equal("age", 6000), equal("age", 7000)), 2);
        assertQuery(map, greaterEqual("age", 5000), 5000);
        assertQuery(map, lessThan("age", 9000), 9000);
        assertQuery(map, lessEqual("age", 9000), 9001);
        assertQuery(map, between("age", 9000, 10000), 1000);
        assertQuery(map, in("age", 5000, 6000, 7000, 11111), 3);
        assertQuery(map, equal("age", "6000"), 1);
        assertQuery(map, like("name", "9999"), 1);

        // open indexer latch
        Person.indexerLatch.countDown();

        // wait for indexer to finish
        assertJoinable(indexer);

        // assert no unexpected exceptions
        assertNull(exception.get());
    }

    private void assertQuery(IMap<Integer, Person> map, Predicate predicate, int expected) {
        Collection<Person> persons = map.values(predicate);
        assertEquals(expected, persons.size());
    }

}
