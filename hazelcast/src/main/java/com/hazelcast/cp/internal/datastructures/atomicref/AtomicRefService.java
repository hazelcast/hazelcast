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

package com.hazelcast.cp.internal.datastructures.atomicref;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.atomicref.proxy.AtomicRefProxy;
import com.hazelcast.cp.internal.datastructures.snapshot.ChunkUtil;
import com.hazelcast.cp.internal.datastructures.snapshot.DataChunkGroup;
import com.hazelcast.cp.internal.datastructures.snapshot.ValueDataChunk;
import com.hazelcast.cp.internal.datastructures.spi.atomic.RaftAtomicValueService;
import com.hazelcast.cp.internal.raft.ChunkedSnapshotAwareService;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.cp.internal.datastructures.snapshot.ChunkUtil.groupServiceChunksBySize;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_TAG_GROUP;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_TAG_NAME;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.spi.properties.ClusterProperty.METRICS_DATASTRUCTURES;

/**
 * Contains Raft-based atomic reference instances, implements snapshotting,
 * and creates proxies
 */
public class AtomicRefService extends RaftAtomicValueService<Data, AtomicRef, AtomicRefSnapshot>
        implements DynamicMetricsProvider, ChunkedSnapshotAwareService<ValueDataChunk, AtomicRefSnapshot> {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = AtomicRefServiceUtil.SERVICE_NAME;
    private long maxChunkSizeInBytes;

    public AtomicRefService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        super.init(nodeEngine, properties);

        if (nodeEngine.getProperties().getBoolean(METRICS_DATASTRUCTURES)) {
            nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(this);
        }

        this.maxChunkSizeInBytes = ChunkUtil.getMaxChunkSizeInBytes(nodeEngine.getProperties());
    }

    @Override
    protected AtomicRefSnapshot newSnapshot(Map<String, Data> values, Set<String> destroyed) {
        return new AtomicRefSnapshot(values, destroyed);
    }

    @Override
    protected AtomicRef newAtomicValue(CPGroupId groupId, String name, Data val) {
        return new AtomicRef(groupId, name, val);
    }

    @Override
    protected IAtomicReference newRaftAtomicProxy(NodeEngine nodeEngine, RaftGroupId groupId, String proxyName,
                                                  String objectNameForProxy) {
        return new AtomicRefProxy(nodeEngine, groupId, proxyName, objectNameForProxy);
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        MetricDescriptor root = descriptor.withPrefix("cp.atomicref");
        Map<CPGroupId, Long> liveCounts = new HashMap<>();
        for (AtomicRef value : atomicValues.values()) {
            CPGroupId groupId = value.groupId();
            String groupName = groupId.getName();
            MetricDescriptor desc = root.copy()
                    .withDiscriminator("id", value.name() + "@" + groupName)
                    .withTag(CP_TAG_NAME, value.name())
                    .withTag(CP_TAG_GROUP, groupName)
                    .withMetric("dummy");
            context.collect(desc, 0);
            liveCounts.merge(groupId, 1L, Long::sum);
        }
        addSummaryMetrics(root.withPrefix("cp.atomicref.summary"), context, liveCounts);
    }

    @Override
    public Iterator<DataChunkGroup<ValueDataChunk>> takeSnapshotChunks(@Nonnull CPGroupId groupId, long commitIndex) {
        AtomicRefSnapshot snapshot = takeSnapshot(groupId, commitIndex);
        return groupServiceChunksBySize(snapshot.toChunks(), maxChunkSizeInBytes).iterator();
    }

    @Override
    public void restoreSnapshotChunk(@Nonnull CPGroupId groupId, long commitIndex,
                                     @Nonnull DataChunkGroup<ValueDataChunk> dataChunk) {
        checkNotNull(groupId);
        checkNotNull(dataChunk);

        List<ValueDataChunk> data = dataChunk.getServiceData();
        for (ValueDataChunk valueData : data) {
            if (valueData.isDestroyed()) {
                if (destroyedValues.add(BiTuple.of(groupId, valueData.getName()))) {
                    destroyedValueCounts.merge(groupId, 1L, Long::sum);
                }
            } else {
                Data value = valueData.getValue();
                String name = valueData.getName();
                atomicValues.put(BiTuple.of(groupId, name), newAtomicValue(groupId, name, value));
            }
        }
    }

    @Override
    public void prepareForSnapshotRestore(@Nonnull CPGroupId groupId) {
        reset();
    }
}
