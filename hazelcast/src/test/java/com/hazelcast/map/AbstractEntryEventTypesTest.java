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

package com.hazelcast.map;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

/**
 * Common superclass to test types of events published with different filtering strategies (default & query cache natural event
 * types)
 */
public abstract class AbstractEntryEventTypesTest extends HazelcastTestSupport {

    @Parameters(name = "includeValues: {0}")
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {true},
                {false},
        });
    }

    @Parameter
    public boolean includeValue;

    @SuppressWarnings("unchecked")
    final Predicate<Integer, Person> predicate = Predicates.sql("age > 50");

    final AtomicInteger eventCounter = new AtomicInteger();
    HazelcastInstance instance;
    IMap<Integer, Person> map;

    // behavior varies between default & query-cache filtering strategies, so individual tests should implement these methods
    abstract MapListener mapListenerFor_entryUpdatedEvent_whenOldValueOutside_newValueMatchesPredicate();

    abstract MapListener mapListenerFor_entryUpdatedEvent_whenOldValueMatches_newValueOutsidePredicate();

    abstract Integer expectedCountFor_entryUpdatedEvent_whenOldValueMatches_newValueOutsidePredicate();

    public static class CountEntryAddedListener implements EntryAddedListener<Integer, Person> {

        private final AtomicInteger counter;

        CountEntryAddedListener(AtomicInteger counter) {
            this.counter = counter;
        }

        @Override
        public void entryAdded(EntryEvent<Integer, Person> event) {
            counter.incrementAndGet();
        }
    }

    public static class CountEntryRemovedListener implements EntryRemovedListener<Integer, Person> {

        private final AtomicInteger counter;

        CountEntryRemovedListener(AtomicInteger counter) {
            this.counter = counter;
        }

        @Override
        public void entryRemoved(EntryEvent<Integer, Person> event) {
            counter.incrementAndGet();
        }
    }

    public static class CountEntryUpdatedListener implements EntryUpdatedListener<Integer, Person> {

        private final AtomicInteger counter;

        CountEntryUpdatedListener(AtomicInteger counter) {
            this.counter = counter;
        }

        @Override
        public void entryUpdated(EntryEvent<Integer, Person> event) {
            counter.incrementAndGet();
        }
    }

    public static class Person implements Serializable {

        String name;
        int age;

        public Person() {
        }

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }

    @Test
    public void entryAddedEvent_whenNoPredicateConfigured() {
        map.addEntryListener(new CountEntryAddedListener(eventCounter), includeValue);
        map.put(1, new Person("a", 40));
        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call()
                    throws Exception {
                return eventCounter.get();
            }
        }, 1);
    }

    @Test
    public void entryAddedEvent_whenValueMatchesPredicate() {
        map.addEntryListener(new CountEntryAddedListener(eventCounter), predicate, includeValue);
        map.put(1, new Person("a", 75));
        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call()
                    throws Exception {
                return eventCounter.get();
            }
        }, 1);
    }

    @Test
    public void entryAddedEvent_whenValueOutsidePredicate() {
        map.addEntryListener(new CountEntryAddedListener(eventCounter), predicate, includeValue);
        map.put(1, new Person("a", 35));
        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call()
                    throws Exception {
                return eventCounter.get();
            }
        }, 0);
    }

    @Test
    public void entryRemovedEvent_whenNoPredicateConfigured() {
        map.addEntryListener(new CountEntryRemovedListener(eventCounter), includeValue);
        map.put(1, new Person("a", 40));
        map.remove(1);
        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call()
                    throws Exception {
                return eventCounter.get();
            }
        }, 1);
    }

    @Test
    public void entryRemovedEvent_whenValueMatchesPredicate() {
        map.addEntryListener(new CountEntryRemovedListener(eventCounter), predicate, includeValue);
        map.put(1, new Person("a", 55));
        map.remove(1);
        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call()
                    throws Exception {
                return eventCounter.get();
            }
        }, 1);
    }

    @Test
    public void entryRemovedEvent_whenValueOutsidePredicate() {
        map.addEntryListener(new CountEntryRemovedListener(eventCounter), predicate, includeValue);
        map.put(1, new Person("a", 35));
        map.remove(1);
        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call()
                    throws Exception {
                return eventCounter.get();
            }
        }, 0);
    }

    @Test
    public void entryUpdatedEvent_whenNoPredicateConfigured() {
        map.addEntryListener(new CountEntryUpdatedListener(eventCounter), includeValue);
        map.put(1, new Person("a", 30));
        map.put(1, new Person("a", 60));

        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call()
                    throws Exception {
                return eventCounter.get();
            }
        }, 1);
    }

    @Test
    public void entryUpdatedEvent_whenOldValueOutside_newValueMatchesPredicate() {
        map.addEntryListener(mapListenerFor_entryUpdatedEvent_whenOldValueOutside_newValueMatchesPredicate(),
                predicate, includeValue);
        map.put(1, new Person("a", 30));
        map.put(1, new Person("a", 60));

        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call()
                    throws Exception {
                return eventCounter.get();
            }
        }, 1);
    }

    @Test
    public void entryUpdatedEvent_whenOldValueOutside_newValueOutsidePredicate() {
        map.addEntryListener(new CountEntryUpdatedListener(eventCounter), predicate, includeValue);
        map.put(1, new Person("a", 30));
        map.put(1, new Person("a", 20));

        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call()
                    throws Exception {
                return eventCounter.get();
            }
        }, 0);
    }

    @Test
    public void entryUpdatedEvent_whenOldValueMatches_newValueMatchesPredicate() {
        map.addEntryListener(new CountEntryUpdatedListener(eventCounter), predicate, includeValue);
        map.put(1, new Person("a", 59));
        map.put(1, new Person("a", 60));

        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call()
                    throws Exception {
                return eventCounter.get();
            }
        }, 1);
    }

    @Test
    public void entryUpdatedEvent_whenOldValueMatches_newValueOutsidePredicate() {
        map.addEntryListener(mapListenerFor_entryUpdatedEvent_whenOldValueMatches_newValueOutsidePredicate(),
                predicate, includeValue);
        map.put(1, new Person("a", 59));
        map.put(1, new Person("a", 30));

        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call()
                    throws Exception {
                return eventCounter.get();
            }
        }, expectedCountFor_entryUpdatedEvent_whenOldValueMatches_newValueOutsidePredicate());
    }
}
