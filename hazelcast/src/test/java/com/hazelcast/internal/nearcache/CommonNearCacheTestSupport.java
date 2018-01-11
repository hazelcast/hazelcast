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

package com.hazelcast.internal.nearcache;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.nearcache.impl.store.NearCacheDataRecordStore;
import com.hazelcast.internal.nearcache.impl.store.NearCacheObjectRecordStore;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.spi.impl.executionservice.impl.DelegatingTaskScheduler;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public abstract class CommonNearCacheTestSupport extends HazelcastTestSupport {

    protected static final int DEFAULT_RECORD_COUNT = 100;
    protected static final String DEFAULT_NEAR_CACHE_NAME = "TestNearCache";

    protected List<ScheduledExecutorService> scheduledExecutorServices = new ArrayList<ScheduledExecutorService>();
    protected SerializationService ss = new DefaultSerializationServiceBuilder()
            .setVersion(InternalSerializationService.VERSION_1).build();

    @After
    public final void shutdownExecutorServices() {
        for (ScheduledExecutorService scheduledExecutorService : scheduledExecutorServices) {
            scheduledExecutorService.shutdown();
        }
        scheduledExecutorServices.clear();
    }

    protected NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        return new NearCacheConfig()
                .setName(name)
                .setInMemoryFormat(inMemoryFormat);
    }

    protected <K, V> NearCacheRecordStore<K, V> createNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                                                           InMemoryFormat inMemoryFormat) {
        NearCacheRecordStore<K, V> recordStore = null;
        switch (inMemoryFormat) {
            case BINARY:
                recordStore = new NearCacheDataRecordStore<K, V>(DEFAULT_NEAR_CACHE_NAME, nearCacheConfig, ss, null);
                break;
            case OBJECT:
                recordStore = new NearCacheObjectRecordStore<K, V>(DEFAULT_NEAR_CACHE_NAME, nearCacheConfig, ss, null);
                break;
            default:
                throw new IllegalArgumentException("Unsupported in-memory format: " + inMemoryFormat);
        }
        recordStore.initialize();

        return recordStore;
    }

    protected TaskScheduler createTaskScheduler() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorServices.add(scheduledExecutorService);
        return new DelegatingTaskScheduler(scheduledExecutorService, scheduledExecutorService);
    }
}
