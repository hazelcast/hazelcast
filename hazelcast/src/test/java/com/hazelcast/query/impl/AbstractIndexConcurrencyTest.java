/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public abstract class AbstractIndexConcurrencyTest extends HazelcastTestSupport {

    private static final int QUERY_THREADS_NUM = 5;

    @Before
    public void setUp() {
        // by default disable awaiting on latch
        Person.accessCountDown = null;
    }

    @Test
    public void testIndexCreationAndQueryConcurrency() throws InterruptedException {
        Config config = getConfig();

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Person> map = node.getMap(randomMapName());

        // put some data
        for (int i = 0; i < 10000; ++i) {
            map.put(i, new Person(i));
        }

        AtomicReference<Throwable> exception = new AtomicReference<>();

        // run index creation and queries concurrently
        Thread[] threads = new Thread[QUERY_THREADS_NUM + 1];

        threads[0] = new Thread(() -> {
            try {
                map.addIndex(IndexType.SORTED, "age");
            } catch (Throwable t) {
                exception.compareAndSet(null, t);
            }
        });
        threads[0].start();

        for (int i = 1; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    Collection<Person> persons = map.values(new SqlPredicate("age >= 5000"));
                    assertEquals(5000, persons.size());
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                }
            });
            threads[i].start();
        }

        // wait for for all threads to finish
        for (int i = 0; i < threads.length; ++i) {
            threads[i].join();
        }

        // assert no unexpected exceptions
        assertNull(exception.get());
    }

    static class Person implements Serializable {

        static AtomicLong accessCountDown;
        static final CountDownLatch indexerLatch = new CountDownLatch(1);
        static final CountDownLatch queryLatch = new CountDownLatch(1);

        public final Integer age;

        Person(Integer value) {
            this.age = value;
        }

        public Integer getAge() {
            if (accessCountDown != null && accessCountDown.decrementAndGet() <= 0) {
                queryLatch.countDown();
                assertOpenEventually(indexerLatch);
            }
            return age;
        }
    }
}
