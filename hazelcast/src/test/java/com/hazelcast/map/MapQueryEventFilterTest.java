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

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapListenerAdapter;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.map.impl.event.MapEventPublisherImpl.PROP_LISTENER_WITH_PREDICATE_PRODUCES_NATURAL_EVENT_TYPES;
import static com.hazelcast.query.Predicates.greaterEqual;
import static junit.framework.TestCase.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapQueryEventFilterTest extends HazelcastTestSupport {

    @Test
    public void test_add_and_update_event_counts_when_natural_event_types_enabled() {
        // expect 1 add event when age = 3 because of natural filtering
        // expect 1 update event when age >= 3
        test_expected_event_types(true, 1, 1);
    }

    /**
     * This tests default behaviour.
     */
    @Test
    public void test_add_and_update_event_counts_when_natural_event_types_disabled() {
        // expect 0 add event when age >= 3
        // expect 2 update events when age >= 3 because of disabled natural filtering
        test_expected_event_types(false, 0, 2);
    }

    private void test_expected_event_types(boolean withNaturalEventType, final int numOfAddEventExpected,
                                           final int numOfUpdateEventExpected) {
        Config config = getConfig();
        config.setProperty(PROP_LISTENER_WITH_PREDICATE_PRODUCES_NATURAL_EVENT_TYPES,
                String.valueOf(withNaturalEventType));
        HazelcastInstance node = createHazelcastInstance(config);

        final AtomicInteger addedEntryCount = new AtomicInteger();
        final AtomicInteger updatedEntryCount = new AtomicInteger();

        IMap<Integer, Employee> map = node.getMap("test");

        map.addEntryListener(new MapListenerAdapter<Integer, Employee>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Employee> event) {
                addedEntryCount.incrementAndGet();
            }

            @Override
            public void entryUpdated(EntryEvent<Integer, Employee> event) {
                updatedEntryCount.incrementAndGet();
            }
        }, greaterEqual("age", 3), true);

        // update same key, multiple times.
        int key = 1;
        map.set(key, new Employee(1));
        map.set(key, new Employee(2));
        map.set(key, new Employee(3));
        map.set(key, new Employee(4));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(numOfAddEventExpected, addedEntryCount.get());
                assertEquals(numOfUpdateEventExpected, updatedEntryCount.get());
            }
        });
    }

    static final class Employee implements Serializable {
        int age;

        Employee(int age) {
            this.age = age;
        }
    }
}
