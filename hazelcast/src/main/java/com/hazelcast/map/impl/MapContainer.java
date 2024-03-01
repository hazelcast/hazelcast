/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.map.impl.record.RecordFactoryAttributes;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.wan.impl.DelegatingWanScheme;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public interface MapContainer {
    void init();

    AtomicLong getLastInvalidMergePolicyCheckTime();

    void initEvictor();

    boolean shouldUseGlobalIndex();

    int getTotalBackupCount();

    int getBackupCount();

    int getAsyncBackupCount();

    PartitioningStrategy getPartitioningStrategy();

    MapServiceContext getMapServiceContext();

    MapStoreContext getMapStoreContext();

    MapConfig getMapConfig();

    void setMapConfig(MapConfig mapConfig);

    EventJournalConfig getEventJournalConfig();

    String getName();

    String getSplitBrainProtectionName();

    Function<Object, Data> toData();

    ConstructorFunction<RecordFactoryAttributes, RecordFactory> getRecordFactoryConstructor();

    QueryableEntry newQueryEntry(Data key, Object value);

    Evictor getEvictor();

    // only used for testing purposes
    void setEvictor(Evictor evictor);

    Extractors getExtractors();

    boolean hasInvalidationListener();

    AtomicInteger getInvalidationListenerCounter();

    InterceptorRegistry getInterceptorRegistry();

    /**
     * Callback invoked before record store and indexes are destroyed. Ensures that if map iterator observes a non-destroyed
     * state, then associated data structures are still valid.
     */
    void onBeforeDestroy();

    // callback called when the MapContainer is de-registered
    // from MapService and destroyed - basically on map-destroy
    void onDestroy();
    boolean isDestroyed();

    boolean shouldCloneOnEntryProcessing(int partitionId);

    ObjectNamespace getObjectNamespace();
    Map<String, IndexConfig> getIndexDefinitions();

    boolean isPersistWanReplicatedData();

    boolean isUseCachedDeserializedValuesEnabled(int partitionId);

    boolean isGlobalIndexEnabled();

    DelegatingWanScheme getWanReplicationDelegate();

    SplitBrainMergePolicy getWanMergePolicy();

    /**
     * @param global set {@code true} to create global indexes, otherwise set
     *               {@code false} to have partitioned indexes
     * @return a new Indexes object
     */
    Indexes createIndexes(boolean global);

    boolean isWanReplicationEnabled();

    boolean isWanRepublishingEnabled();

    void initWanReplication();

    /**
     * @return the global index, if the global index is in use or null.
     */
    Indexes getIndexes();

    /**
     * @param partitionId partitionId
     */
    Indexes getIndexes(int partitionId);
}
