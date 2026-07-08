/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl;

import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.internal.memory.Measurable;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.impl.VectorCollectionServiceUtil;
import com.hazelcast.vector.internal.impl.query.Searcher;
import com.hazelcast.vector.internal.impl.stats.CollectionOnDemandStats;
import com.hazelcast.vector.internal.impl.stats.LocalVectorCollectionStatsImpl;
import com.hazelcast.vector.internal.impl.storage.VectorCollectionStorage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

public interface VectorCollectionService extends ManagedService, RemoteService, Measurable {
    /** service name */
    String SERVICE_NAME = VectorCollectionServiceUtil.SERVICE_NAME;

    /**
     * Returns storage for vector collection partition, creating it if needed
     */
    @Nonnull
    VectorCollectionStorage getStorage(String vectorCollectionName, int partitionId);

    /**
     * Returns storage for vector collection partition or null if the collection or partition
     * does not exist.
     */
    @Nullable
    VectorCollectionStorage getStorageOrNull(String vectorCollectionName, int partitionId);

    /**
     * Creates {@link VectorCollectionStorage} but does not make it visible to others via
     * {@link #getStorage} or {@link #getStorageOrNull}.
     *
     * @see #attachStorage
     */
    @Nonnull
    VectorCollectionStorage createStorage(String vectorCollectionName, int partitionId);

    /**
     * Attaches previously created storage to the service and makes it visible via
     * {@link #getStorage} or {@link #getStorageOrNull}.
     *
     * @see #createStorage
     */
    void attachStorage(@Nonnull VectorCollectionStorage collectionStorage);

    /**
     * Destroys and removes storage of vector collection partition, if exists.
     */
    void destroyStorage(String vectorCollectionName, int partitionId);

    /**
     * Provides searcher instance for given collection and options.
     * This method may reuse the same instance if that is safe.
     */
    @Nonnull
    Searcher getSearcher(String vectorCollectionName, SearchOptions options);

    /**
     * Provides statistics object instance for given vector collection.
     * The instance is shared by all partitions.
     * This method is intended to be used for updating the statistics.
     * It is cheap but returned object does not have up-to-date on-demand
     * calculated statistics (e.g. size or heap usage).
     *
     * @see #getOnDemandStats
     */
    @Nonnull
    LocalVectorCollectionStatsImpl getStatistics(String vectorCollectionName);

    /**
     * Provides on demand statistics for given vector collection.
     */
    @Nonnull
    CollectionOnDemandStats getOnDemandStats(String vectorCollectionName);

    /**
     * @return names of known existing {@code VectorCollection}s
     */
    @Nonnull
    Set<String> getAllExistingVectorCollectionNames();

    /**
     * @return object namespace for given collection
     * @implNote may cache the namespaces to avoid creating garbage
     */
    ObjectNamespace getObjectNamespace(String collectionName);

    /**
     * @return vector collection optimizations manager
     */
    VectorCollectionOptimizationManager getOptimizationManager();

    static String lookupNamespace(@Nonnull NodeEngine engine, @Nonnull String collectionName) {
        if (!engine.getNamespaceService().isEnabled()) {
            return null;
        }
        VectorCollectionConfig collectionConfig = engine.getConfig().getVectorCollectionConfigOrNull(collectionName);
        return collectionConfig == null ? null : collectionConfig.getUserCodeNamespace();
    }
}
