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

package com.hazelcast.cache.impl.event;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

/**
 * This interface provides methods to publish wan replication events
 * from cache operations.
 * <p>
 * Methods of this class should be called from within the partition thread
 * to keep event order for keys belonging to a single partition.
 */
public interface CacheWanEventPublisher {

    /**
     * This method will create a wrapper object using the given {@link CacheEntryView}
     * and place it to wan replication queues.
     * <p>
     * Updating cache operations should call this method in their {@link Operation#afterRun()} method.
     *
     * @param cacheNameWithPrefix the full name of the {@link com.hazelcast.cache.ICache}, including the manager scope prefix
     * @param entryView           the updated cache entry
     * @see com.hazelcast.cache.impl.operation.CachePutOperation
     * @see com.hazelcast.cache.impl.operation.CacheGetAndReplaceOperation
     */
    void publishWanReplicationUpdate(String cacheNameWithPrefix, CacheEntryView<Data, Data> entryView);

    /**
     * This method will create a wrapper object using the given {@link CacheEntryView}
     * and place it to wan replication queues.
     * <p>
     * Cache operations which removes data from cache should call this method in their
     * {@link Operation#afterRun()} method.
     *
     * @param cacheNameWithPrefix the full name of the {@link com.hazelcast.cache.ICache}, including the manager scope prefix
     * @param key                 the key of the removed entry
     * @see com.hazelcast.cache.impl.operation.CacheRemoveOperation
     */
    void publishWanReplicationRemove(String cacheNameWithPrefix, Data key);

    /**
     * Backup operations of operations that call {@link this#publishWanReplicationUpdate(String, CacheEntryView)}
     * should call this method to provide wan event backups
     *
     * @param cacheNameWithPrefix the full name of the {@link com.hazelcast.cache.ICache}, including the manager scope prefix
     * @param entryView           the updated cache entry
     * @see com.hazelcast.cache.impl.operation.CachePutBackupOperation
     */
    void publishWanReplicationUpdateBackup(String cacheNameWithPrefix, CacheEntryView<Data, Data> entryView);

    /**
     * Backup operations of operations that call {@link this#publishWanReplicationRemove(String, Data)}
     * should call this method to provide wan event backups
     *
     * @param cacheNameWithPrefix the full name of the {@link com.hazelcast.cache.ICache}, including the manager scope prefix
     * @param key                 the key of the removed entry
     * @see com.hazelcast.cache.impl.operation.CacheRemoveBackupOperation
     */
    void publishWanReplicationRemoveBackup(String cacheNameWithPrefix, Data key);

}
