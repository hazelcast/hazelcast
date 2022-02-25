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

package com.hazelcast.internal.nearcache.impl.invalidation;


import com.hazelcast.internal.nearcache.NearCacheRecord;

/**
 * Default implementation of {@link StaleReadDetector}.
 */
public class StaleReadDetectorImpl implements StaleReadDetector {

    private final RepairingHandler repairingHandler;
    private final MinimalPartitionService partitionService;

    StaleReadDetectorImpl(RepairingHandler repairingHandler, MinimalPartitionService partitionService) {
        this.repairingHandler = repairingHandler;
        this.partitionService = partitionService;
    }

    @Override
    public boolean isStaleRead(Object key, NearCacheRecord record) {
        MetaDataContainer latestMetaData = repairingHandler.getMetaDataContainer(record.getPartitionId());
        return !record.hasSameUuid(latestMetaData.getUuid())
                || record.getInvalidationSequence() < latestMetaData.getStaleSequence();
    }

    @Override
    public int getPartitionId(Object key) {
        return partitionService.getPartitionId(key);
    }

    @Override
    public MetaDataContainer getMetaDataContainer(int partitionId) {
        return repairingHandler.getMetaDataContainer(partitionId);
    }

    @Override
    public String toString() {
        return "Default StaleReadDetectorImpl";
    }
}
