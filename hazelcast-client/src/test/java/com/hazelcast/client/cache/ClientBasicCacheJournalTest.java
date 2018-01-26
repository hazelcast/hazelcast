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

package com.hazelcast.client.cache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.journal.BasicCacheJournalTest;
import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.client.cache.impl.ClientCacheProxy;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.journal.EventJournalInitialSubscriberState;
import com.hazelcast.projection.Projection;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.function.Predicate;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientBasicCacheJournalTest extends BasicCacheJournalTest {

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
    protected CacheManager createCacheManager() {
        final HazelcastClientCachingProvider provider = HazelcastClientCachingProvider.createCachingProvider(client);
        return provider.getCacheManager();
    }

    @Override
    protected <K, V> ICache<K, V> getCache(String cacheName) {
        return (ICache<K, V>) cacheManager.getCache(cacheName);
    }

    @Override
    protected <K, V> EventJournalInitialSubscriberState subscribeToEventJournal(Cache<K, V> cache, int partitionId) throws Exception {
        return ((ClientCacheProxy<K, V>) cache).subscribeToEventJournal(partitionId).get();
    }

    @Override
    protected <K, V, T> ICompletableFuture<ReadResultSet<T>> readFromEventJournal(Cache<K, V> cache,
                                                                                  long startSequence,
                                                                                  int maxSize,
                                                                                  int partitionId,
                                                                                  Predicate<? super EventJournalCacheEvent<K, V>> predicate,
                                                                                  Projection<? super EventJournalCacheEvent<K, V>, T> projection) {
        return ((ClientCacheProxy<K, V>) cache).readFromEventJournal(startSequence, 1, maxSize, partitionId, predicate, projection);
    }

    @After
    public final void terminate() {
        factory.terminateAll();
    }
}
