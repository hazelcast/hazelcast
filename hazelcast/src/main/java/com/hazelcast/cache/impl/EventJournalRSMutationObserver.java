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

package com.hazelcast.cache.impl;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.ObjectNamespace;

public class EventJournalRSMutationObserver implements CacheRSMutationObserver {

    private final EventJournalConfig eventJournalConfig;
    private final ObjectNamespace objectNamespace;
    private final AbstractCacheService cacheService;
    private final int partitionId;

    public EventJournalRSMutationObserver(AbstractCacheService cacheService,
                                          EventJournalConfig eventJournalConfig,
                                          ObjectNamespace objectNamespace, int partitionId) {
        this.eventJournalConfig = eventJournalConfig;
        this.objectNamespace = objectNamespace;
        this.cacheService = cacheService;
        this.partitionId = partitionId;
    }

    @Override
    public void onCreate(Data key, Object value) {
        cacheService.eventJournal.writeCreatedEvent(eventJournalConfig, objectNamespace, partitionId, key, value);
    }

    @Override
    public void onRemove(Data key, Object value) {
        cacheService.eventJournal.writeRemoveEvent(eventJournalConfig, objectNamespace, partitionId, key, value);
    }

    @Override
    public void onUpdate(Data key, Object oldValue, Object value) {
        cacheService.eventJournal.writeUpdateEvent(eventJournalConfig, objectNamespace, partitionId, key, oldValue, value);
    }

    @Override
    public void onEvict(Data key, Object value) {
        cacheService.eventJournal.writeEvictEvent(eventJournalConfig, objectNamespace, partitionId, key, value);
    }

    @Override
    public void onExpire(Data key, Object value) {
        cacheService.eventJournal.writeExpiredEvent(eventJournalConfig, objectNamespace, partitionId, key, value);
    }

    @Override
    public void onDestroy() {
        cacheService.eventJournal.destroy(objectNamespace, partitionId);
    }
}
