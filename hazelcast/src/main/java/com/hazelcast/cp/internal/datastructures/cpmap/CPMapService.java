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

package com.hazelcast.cp.internal.datastructures.cpmap;

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftNodeLifecycleAwareService;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapOperationProvider;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapOperationProviderImpl;
import com.hazelcast.cp.internal.datastructures.cpmap.proxy.CPMapProxy;
import com.hazelcast.cp.internal.datastructures.cpmap.store.CPMapStore;
import com.hazelcast.cp.internal.datastructures.snapshot.ChunkUtil;
import com.hazelcast.cp.internal.datastructures.snapshot.DataChunkGroup;
import com.hazelcast.cp.internal.datastructures.snapshot.KeyValueDataChunk;
import com.hazelcast.cp.internal.datastructures.spi.RaftManagedService;
import com.hazelcast.cp.internal.datastructures.spi.RaftRemoteService;
import com.hazelcast.cp.internal.raft.ChunkedSnapshotAwareService;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.cp.internal.datastructures.cpmap.store.CPMapMetricDescriptorConstants.CP_MAP_ROOT_PREFIX;
import static com.hazelcast.cp.internal.datastructures.cpmap.store.CPMapMetricDescriptorConstants.CP_MAP_SUMMARY;
import static com.hazelcast.cp.internal.datastructures.snapshot.ChunkUtil.groupServiceChunksBySize;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_SUMMARY_DESTROYED_COUNT;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_SUMMARY_LIVE_COUNT;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_TAG_GROUP;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.spi.properties.ClusterProperty.METRICS_DATASTRUCTURES;

public class CPMapService implements RaftManagedService, RaftRemoteService,
        ChunkedSnapshotAwareService<KeyValueDataChunk, CPMapRegistrySnapshot>,
        RaftNodeLifecycleAwareService, DynamicMetricsProvider {
    /**
     * Name of the registered CP map service.
     */
    public static final String SERVICE_NAME = CPMapServiceUtil.SERVICE_NAME;

    protected final NodeEngine nodeEngine;

    protected volatile RaftService raftService;

    private final ConcurrentMap<CPGroupId, CPMapRegistry> mapRegistries = new ConcurrentHashMap<>();
    private final CPSubsystemConfig cpSubsystemConfig;
    private final long maxChunkSizeInBytes;
    private final CPMapOperationProvider cpMapOperationProvider;

    public CPMapService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.cpSubsystemConfig = nodeEngine.getConfig().getCPSubsystemConfig();
        this.maxChunkSizeInBytes = ChunkUtil.getMaxChunkSizeInBytes(nodeEngine.getProperties());
        this.cpMapOperationProvider = new CPMapOperationProviderImpl();
    }

    public CPMapOperationProvider getCpMapOperationProvider(boolean purgeEnabled) {
        return cpMapOperationProvider;
    }

    public CPMapStore getOrInitMapStore(@Nonnull CPGroupId groupId, @Nonnull String objectName) {
        CPMapRegistry registry = mapRegistries.get(groupId);

        if (registry == null) {
            registry = mapRegistries.computeIfAbsent(groupId,
                    r -> createCPMapRegistry(groupId));
        }

        return registry.getMapStore(nodeEngine, objectName, cpSubsystemConfig);
    }

    protected CPMapRegistry createCPMapRegistry(@Nonnull CPGroupId groupId) {
        return new CPMapRegistry(groupId);
    }

    public CPMapStore getExistingMapStoreOrNull(@Nonnull CPGroupId groupId, @Nonnull String objectName) {
        CPMapRegistry cpMapRegistry = mapRegistries.get(groupId);
        if (cpMapRegistry == null) {
            return null;
        }

        CPMapStore cpMapStore = cpMapRegistry.getMapStores().get(objectName);
        if (cpMapStore == null) {
            return null;
        }

        return cpMapStore;
    }

    public Collection<String> listResourceNames(CPGroupId groupId, boolean tombstone) {
        CPMapRegistry cpMapRegistry = mapRegistries.get(groupId);
        if (cpMapRegistry == null) {
            return List.of();
        }

        if (tombstone) {
            return cpMapRegistry.getDestroyed();
        }

        return cpMapRegistry.getMapStores().keySet();
    }

    @Override
    public void onCPSubsystemRestart() {
        mapRegistries.clear();
    }

    @Override
    public DistributedObject createProxy(String objectName) {
        CPGroupId groupId = raftService.createRaftGroupForProxy(objectName);
        return new CPMapProxy<>(nodeEngine, groupId, objectName);
    }

    @Override
    public boolean destroyRaftObject(CPGroupId groupId, String objectName) {
        return mapRegistries.computeIfAbsent(groupId,
                r -> new CPMapRegistry(groupId)).destroyMapStore(objectName);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        raftService = nodeEngine.getService(RaftService.SERVICE_NAME);

        if (nodeEngine.getProperties().getBoolean(METRICS_DATASTRUCTURES)
                && nodeEngine instanceof NodeEngineImpl nodeEngineImpl) {
            nodeEngineImpl.getMetricsRegistry().registerDynamicMetricsProvider(this);
        }
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        mapRegistries.clear();
    }

    @Override
    public Iterator<DataChunkGroup<KeyValueDataChunk>> takeSnapshotChunks(@Nonnull CPGroupId groupId,
                                                                          long commitIndex) {
        checkNotNull(groupId);

        CPMapRegistrySnapshot snapshot = takeSnapshot(groupId, commitIndex);
        return groupServiceChunksBySize(snapshot.toChunks(maxChunkSizeInBytes), maxChunkSizeInBytes).iterator();
    }

    @Override
    public void restoreSnapshotChunk(@Nonnull CPGroupId groupId, long commitIndex,
                                     @Nonnull DataChunkGroup dataChunk) {
        checkNotNull(groupId);
        checkNotNull(dataChunk);

        List<KeyValueDataChunk> cpMapDataList = dataChunk.getServiceData();
        for (KeyValueDataChunk cpMapData : cpMapDataList) {
            String cpMapName = cpMapData.getName();

            if (cpMapData.isDestroyed()) {
                destroyRaftObject(groupId, cpMapName);
            } else {
                CPMapStore cpMapStore = getOrInitMapStore(groupId, cpMapName);
                boolean purgeEnabled = cpMapStore.isPurgeEnabled();
                List<Data> keyValuePairs = getKeyValuePairs(cpMapData, purgeEnabled);

                for (int i = 0; i < keyValuePairs.size(); i += 2) {
                    Data key = keyValuePairs.get(i);
                    Data value = keyValuePairs.get(i + 1);

                    if (purgeEnabled) {
                        Long lastUpdateTime = cpMapData.getEntryTimestamps().get(i / 2);
                        cpMapStore.put(key, value, lastUpdateTime);
                    } else {
                        cpMapStore.put(key, value, CPMapStore.NO_TIMESTAMP);
                    }
                }
            }
        }
    }

    private List<Data> getKeyValuePairs(KeyValueDataChunk cpMapChunk, boolean purgeEnabled) {
        List<Data> keyValuePairs = cpMapChunk.getKeyValuePairs();

        if (purgeEnabled) {
            // Fail fast: this case likely occurs when purge is enabled
            // for an existing map across restarts, which is not supported.
            //
            // The loaded data does not contain timestamps.
            if (!keyValuePairs.isEmpty() && cpMapChunk.getEntryTimestamps().isEmpty()) {
                throw new UnsupportedOperationException(
                        "Purge is enabled but CPMap state is inconsistent: "
                                + "no entry timestamps exist."
                );
            }
        }
        return keyValuePairs;
    }

    @Override
    public void prepareForSnapshotRestore(@Nonnull CPGroupId groupId) {
        assert raftService.isCpSubsystemEnabled() : "no call is expected when cp is disable";

        checkNotNull(groupId);

        CPMapRegistry cpMapRegistry = mapRegistries.get(groupId);
        if (cpMapRegistry == null) {
            return;
        }

        cpMapRegistry.resetForChunkedSnapshotRestore();
    }

    @Override
    public CPMapRegistrySnapshot takeSnapshot(CPGroupId groupId, long commitIndex) {
        CPMapRegistry cpMapRegistry = mapRegistries.get(groupId);
        if (cpMapRegistry == null) {
            return new CPMapRegistrySnapshot();
        }
        return cpMapRegistry.takeSnapshot();
    }

    @Override
    public void onRaftNodeTerminated(CPGroupId groupId) {
        mapRegistries.remove(groupId);
    }

    @Override
    public void onRaftNodeSteppedDown(CPGroupId groupId) {
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        MetricDescriptor root = descriptor.withPrefix(CP_MAP_ROOT_PREFIX);
        for (CPMapRegistry cpMapRegistry : mapRegistries.values()) {
            cpMapRegistry.collectMetrics(root, context);
        }
        addSummaryMetrics(root, context, mapRegistries);
    }

    static void addSummaryMetrics(MetricDescriptor descriptor,
                                  MetricsCollectionContext context,
                                  Map<CPGroupId, CPMapRegistry> mapRegistries) {
        MetricDescriptor destroyed = descriptor.withPrefix(CP_MAP_SUMMARY);
        for (CPMapRegistry cpMapRegistry : mapRegistries.values()) {
            MetricDescriptor destroyedCount = destroyed.copy().withDiscriminator(CP_TAG_GROUP, cpMapRegistry.getGroupName())
                    .withMetric(CP_METRIC_SUMMARY_DESTROYED_COUNT);
            context.collect(destroyedCount, cpMapRegistry.getDestroyed().size());

            MetricDescriptor liveCount = destroyed.copy().withDiscriminator(CP_TAG_GROUP, cpMapRegistry.getGroupName())
                    .withMetric(CP_METRIC_SUMMARY_LIVE_COUNT);
            context.collect(liveCount, cpMapRegistry.getMapStores().size());
        }
    }
}
