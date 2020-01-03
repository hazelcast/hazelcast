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

package com.hazelcast.query.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.impl.predicates.SqlPredicate;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexConcurrencyTest extends AbstractIndexConcurrencyTest {

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
                map.addIndex(IndexType.SORTED, "age");
            } catch (Throwable t) {
                exception.compareAndSet(null, t);
            }
        });
        indexer.start();

        // await for query latch
        assertOpenEventually(Person.queryLatch);
        Person.accessCountDown = null;

        // run checking query
        Collection<Person> persons = map.values(new SqlPredicate("age >= 5000"));
        assertEquals(5000, persons.size());

        // open indexer latch
        Person.indexerLatch.countDown();

        // wait for indexer to finish
        assertJoinable(indexer);

        // assert no unexpected exceptions
        assertNull(exception.get());
    }

}
