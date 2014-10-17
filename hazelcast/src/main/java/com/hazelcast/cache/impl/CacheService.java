/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.operation.CacheReplicationOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentMap;

/**
 * Cache Service is the base access point of jcache impl to manage
 * cache data on a node.
 * This service is an optionally registered service which is enable if the
 * javax.cache.Caching class is found on classpath.
 *
 * If enable this service will provide all cache data operation for all partitions
 * of the node which it is registered on.
 *
 */
public class CacheService extends AbstractCacheService implements ICacheService {

    protected ICacheRecordStore createNewRecordStore(String name, int partitionId) {
        return new CacheRecordStore(name, partitionId, nodeEngine, CacheService.this);
    }

    @Override
    public void reset() {
        final ConcurrentMap<String, CacheConfig> cacheConfigs = configs;
        for (String objectName : cacheConfigs.keySet()) {
            destroyCache(objectName, true, null);
        }
        final CachePartitionSegment[] partitionSegments = segments;
        for (CachePartitionSegment partitionSegment : partitionSegments) {
            if (partitionSegment != null) {
                partitionSegment.clear();
            }
        }
        //TODO: near cache not implemented yet. enable wen ready
        //        for (NearCache nearCache : nearCacheMap.values()) {
        //            nearCache.clear();
        //        }

    }

    @Override
    public void shutdown(boolean terminate) {
        if (!terminate) {
            reset();
        }
    }

    //region MigrationAwareService
    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        CachePartitionSegment segment = segments[event.getPartitionId()];
        CacheReplicationOperation op = new CacheReplicationOperation(segment, event.getReplicaIndex());
        return op.isEmpty() ? null : op;
    }
    //endregion
}
