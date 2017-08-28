/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map;

import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.journal.EventJournalInitialSubscriberState;
import com.hazelcast.map.impl.journal.BasicMapJournalTest;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.projection.Projection;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.util.function.Predicate;
import org.junit.After;

public class ClientBasicMapJournalTest extends BasicMapJournalTest {

    private TestHazelcastFactory factory;
    private HazelcastInstance client;

    @Override
    protected HazelcastInstance getRandomInstance() {
        return client;
    }

    @Override
    protected HazelcastInstance[] createInstances() {
        factory = new TestHazelcastFactory();
        final HazelcastInstance[] instances = factory.newInstances(getConfig(), 2);
        client = factory.newHazelcastClient();
        return instances;
    }

    @Override
    protected <K, V> IMap<K, V> getMap(String mapName) {
        return client.getMap(mapName);
    }

    @Override
    protected <K, V> EventJournalInitialSubscriberState subscribeToEventJournal(IMap<K, V> map, int partitionId) throws Exception{
        return ((ClientMapProxy<K, V>) map).subscribeToEventJournal(partitionId).get();
    }

    @Override
    protected <K, V, T> ICompletableFuture<ReadResultSet<T>> readFromEventJournal(IMap<K, V> map,
                                                                                  long startSequence,
                                                                                  int maxSize,
                                                                                  int partitionId,
                                                                                  Predicate<? super EventJournalMapEvent<K, V>> predicate,
                                                                                  Projection<? super EventJournalMapEvent<K, V>, T> projection) {
        return ((ClientMapProxy<K, V>) map).readFromEventJournal(startSequence, 1, maxSize, partitionId, predicate, projection);
    }

    @After
    public final void terminate() {
        factory.terminateAll();
    }
}
