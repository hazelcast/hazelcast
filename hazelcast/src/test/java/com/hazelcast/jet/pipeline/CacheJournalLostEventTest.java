/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.jet.pipeline;

import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.jet.Util.cachePutEvents;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;

public class CacheJournalLostEventTest extends AbstractJournalLostEventTest {

    private String sourceCache;

    @Before
    public void before() {
        sourceCache = journaledCacheName();
    }

    @Test
    public void defaultProjection1_receivedEvent() {
        performTest(
                Sources.cacheJournalEntries(sourceCache, START_FROM_OLDEST),
                JournalSourceEntry::isAfterLostEvents
        );
    }

    @Test
    public void customProjection_receivedEvent() {
        performTest(
                Sources.cacheJournal(sourceCache, START_FROM_OLDEST, EventJournalCacheEvent::isAfterLostEvents, cachePutEvents()),
                FunctionEx.identity()
        );
    }

    @Test
    public void remoteSourceCustomProjection_receivedEvent() {
        performTest(
                remoteInstance,
                Sources.remoteCacheJournal(sourceCache, remoteHzClientConfig, START_FROM_OLDEST, EventJournalCacheEvent::isAfterLostEvents, cachePutEvents()),
                FunctionEx.identity()
        );
    }

    protected void put(HazelcastInstance hz, Integer key, Integer value) {
        hz.getCacheManager().getCache(sourceCache).put(key, value);
    }
}
