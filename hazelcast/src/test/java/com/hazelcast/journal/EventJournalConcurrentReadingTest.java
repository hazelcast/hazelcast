/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.journal;

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.projection.Projections;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.util.function.Consumer;
import com.hazelcast.util.function.Predicate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class EventJournalConcurrentReadingTest extends HazelcastTestSupport {

    private static final int JOURNAL_CAPACITY = 1000;
    private IMap<String, Integer> map;
    private EventJournalReader<EventJournalMapEvent<String, Integer>> reader;

    @Before
    public void setUp() {
        Config config = new Config();

        EventJournalConfig journalConfig = new EventJournalConfig()
                .setMapName("*")
                .setCapacity(JOURNAL_CAPACITY)
                .setEnabled(true);
        config.addEventJournalConfig(journalConfig);
        config.setProperty("hazelcast.partition.count", "1");
        config.setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "60000");
        HazelcastInstance instance = createHazelcastInstance(config);

        map = instance.getMap(randomMapName());
        reader = (EventJournalReader<EventJournalMapEvent<String, Integer>>) map;
    }

    @Test
    public void test() {
        for (int i = 1; i <= 3; i++) {
            map.put("1", i);
        }

        readFromJournal(0, new Predicate<EventJournalMapEvent<String, Integer>>() {
            @Override public boolean test(EventJournalMapEvent<String, Integer> e) {
                return e.getType() == EntryEventType.ADDED;
            }
        }, new Consumer<EventJournalMapEvent<String, Integer>>() {
            @Override public void accept(EventJournalMapEvent<String, Integer> e) {
            }
        });

        final CopyOnWriteArrayList<Integer> updates = new CopyOnWriteArrayList<Integer>();
        readFromJournal(0, new Predicate<EventJournalMapEvent<String, Integer>>() {
            @Override public boolean test(EventJournalMapEvent<String, Integer> e) {
                return e.getType() == EntryEventType.UPDATED;
            }
        }, new Consumer<EventJournalMapEvent<String, Integer>>() {
            @Override public void accept(EventJournalMapEvent<String, Integer> e) {
                updates.add(e.getNewValue());
                if (e.getNewValue() < 100) {
                    map.put(e.getKey(), e.getNewValue() * 100);
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                System.out.println(updates);
                assertEquals(new HashSet<Integer>(Arrays.asList(2, 3, 200, 300)), new HashSet<Integer>(updates));
            }
        });
    }

    private void readFromJournal(long seq,
                                 final Predicate<EventJournalMapEvent<String, Integer>> predicate,
                                 final Consumer<EventJournalMapEvent<String, Integer>> consumer
    ) {
        ICompletableFuture<ReadResultSet<Object>> future = reader.readFromEventJournal(
                seq,
                1,
                16,
                0,
                predicate, Projections.identity()
        );
        future.andThen(new ExecutionCallback<ReadResultSet<Object>>() {
            @Override
            public void onResponse(ReadResultSet<Object> response) {
                readFromJournal(response.getNextSequenceToReadFrom(), predicate, consumer);
                for (Object event : response) {
                    consumer.accept((EventJournalMapEvent<String, Integer>) event);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                t.printStackTrace();
            }
        });
    }
}
