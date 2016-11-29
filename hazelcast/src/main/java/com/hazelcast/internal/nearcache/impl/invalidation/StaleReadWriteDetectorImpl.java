/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nearcache.impl.invalidation;


import com.hazelcast.internal.nearcache.NearCacheRecord;

import java.util.UUID;

/**
 * Default implementation of {@link StaleReadWriteDetector}
 */
public class StaleReadWriteDetectorImpl implements StaleReadWriteDetector {

    private final RepairingHandler repairingHandler;
    private final MinimalPartitionService partitionService;

    public StaleReadWriteDetectorImpl(RepairingHandler repairingHandler, MinimalPartitionService partitionService) {
        this.repairingHandler = repairingHandler;
        this.partitionService = partitionService;
    }

    @Override
    public long takeSequence(Object key) {
        int partition = partitionService.getPartitionId(key);
        return repairingHandler.getLastReceivedSequence(partition);
    }

    @Override
    public UUID takeUuid(Object key) {
        int partition = partitionService.getPartitionId(key);
        return repairingHandler.getUuid(partition);
    }

    @Override
    public boolean isStaleRead(Object key, NearCacheRecord nearCacheRecord) {
        int partition = partitionService.getPartitionId(key);
        return nearCacheRecord.hasSameUuid(repairingHandler.getUuid(partition))
                && nearCacheRecord.getSequence() < repairingHandler.getLastStaleSequence(partition);
    }

    @Override
    public boolean isStaleWrite(Object key, NearCacheRecord nearCacheRecord) {
        int partition = partitionService.getPartitionId(key);
        return nearCacheRecord.hasSameUuid(repairingHandler.getUuid(partition))
                && nearCacheRecord.getSequence() < repairingHandler.getLastReceivedSequence(partition);
    }
}
