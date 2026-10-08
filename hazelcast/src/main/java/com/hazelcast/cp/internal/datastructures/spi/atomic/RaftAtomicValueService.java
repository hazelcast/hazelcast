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

package com.hazelcast.cp.internal.datastructures.spi.atomic;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftNodeLifecycleAwareService;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.spi.RaftManagedService;
import com.hazelcast.cp.internal.datastructures.spi.RaftRemoteService;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;
import static com.hazelcast.cp.internal.RaftService.withoutDefaultGroupName;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_SUMMARY_DESTROYED_COUNT;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_SUMMARY_LIVE_COUNT;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_TAG_GROUP;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Contains Raft-based atomic value instances, implements snapshotting,
 * and creates proxies
 */
public abstract class RaftAtomicValueService<T, V extends RaftAtomicValue<T>, S extends RaftAtomicValueSnapshot<T>>
        implements RaftManagedService, RaftRemoteService, RaftNodeLifecycleAwareService, SnapshotAwareService<S> {

    protected final Map<BiTuple<CPGroupId, String>, V> atomicValues = new ConcurrentHashMap<>();
    protected final Set<BiTuple<CPGroupId, String>> destroyedValues = ConcurrentHashMap.newKeySet();
    protected final Map<CPGroupId, Long> destroyedValueCounts = new ConcurrentHashMap<>();

    private volatile RaftService raftService;
    private final NodeEngine nodeEngine;

    public RaftAtomicValueService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
    }

    @Override
    public void reset() {
        if (!raftService.isCpSubsystemEnabled()) {
            clearValues();
        }
    }

    private void clearValues() {
        atomicValues.clear();
        destroyedValues.clear();
        destroyedValueCounts.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        clearValues();
    }

    @Override
    public void onCPSubsystemRestart() {
        clearValues();
    }

    @Override
    public final S takeSnapshot(CPGroupId groupId, long commitIndex) {
        checkNotNull(groupId);

        Map<String, T> values = new HashMap<>();
        for (V value : atomicValues.values()) {
            if (value.groupId().equals(groupId)) {
                values.put(value.name(), value.get());
            }
        }

        Set<String> destroyed = new HashSet<>();
        for (BiTuple<CPGroupId, String> tuple : destroyedValues) {
            if (groupId.equals(tuple.element1)) {
                destroyed.add(tuple.element2);
            }
        }

        return newSnapshot(values, destroyed);
    }

    protected abstract S newSnapshot(Map<String, T> values, Set<String> destroyed);

    @Override
    public final void restoreSnapshot(CPGroupId groupId, long commitIndex, S snapshot) {
        checkNotNull(groupId);
        for (Map.Entry<String, T> e : snapshot.getValues()) {
            String name = e.getKey();
            T val = e.getValue();
            atomicValues.put(BiTuple.of(groupId, name), newAtomicValue(groupId, name, val));
        }

        for (String name : snapshot.getDestroyed()) {
            if (destroyedValues.add(BiTuple.of(groupId, name))) {
                destroyedValueCounts.merge(groupId, 1L, Long::sum);
            }
        }
    }

    protected abstract V newAtomicValue(CPGroupId groupId, String name, T val);

    @Override
    public final void onRaftNodeTerminated(CPGroupId groupId) {
        Iterator<BiTuple<CPGroupId, String>> iter = atomicValues.keySet().iterator();
        while (iter.hasNext()) {
            BiTuple<CPGroupId, String> next = iter.next();
            if (groupId.equals(next.element1)) {
                if (destroyedValues.add(next)) {
                    destroyedValueCounts.merge(groupId, 1L, Long::sum);
                }
                iter.remove();
            }
        }
    }

    @Override
    public void onRaftNodeSteppedDown(CPGroupId groupId) {
    }

    @Override
    public final boolean destroyRaftObject(CPGroupId groupId, String name) {
        BiTuple<CPGroupId, String> key = BiTuple.of(groupId, name);
        if (destroyedValues.add(key)) {
            destroyedValueCounts.merge(groupId, 1L, Long::sum);
        }
        return atomicValues.remove(key) != null;
    }

    public int getAtomicValuesCount() {
        return atomicValues.size();
    }

    // squid:S3824 ConcurrentHashMap.computeIfAbsent(K, Function<? super K, ? extends V>) locks the map, which *may* have an
    // effect on throughput such that it's not a direct replacement
    @SuppressWarnings("squid:S3824")
    public final V getAtomicValue(CPGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);
        BiTuple<CPGroupId, String> key = BiTuple.of(groupId, name);
        if (destroyedValues.contains(key)) {
            throw new DistributedObjectDestroyedException("AtomicValue[" + name + "] is already destroyed!");
        }
        V atomicValue = atomicValues.get(key);
        if (atomicValue == null) {
            atomicValue = newAtomicValue(groupId, name, null);
            atomicValues.put(key, atomicValue);
        }
        return atomicValue;
    }

    @Override
    public final DistributedObject createProxy(String proxyName) {
        try {
            proxyName = withoutDefaultGroupName(proxyName);
            RaftGroupId groupId = raftService.createRaftGroupForProxy(proxyName);
            return newRaftAtomicProxy(nodeEngine, groupId, proxyName, getObjectNameForProxy(proxyName));
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected abstract DistributedObject newRaftAtomicProxy(NodeEngine nodeEngine, RaftGroupId groupId,
            String proxyName, String objectNameForProxy);


    protected void addSummaryMetrics(MetricDescriptor descriptor, MetricsCollectionContext context,
                                     Map<CPGroupId, Long> liveCounts) {
        addSummaryMetrics(descriptor, context, liveCounts, destroyedValueCounts);
    }

    static void addSummaryMetrics(MetricDescriptor descriptor, MetricsCollectionContext context,
                                  Map<CPGroupId, Long> liveCounts, Map<CPGroupId, Long> destroyedValueCounts) {
        Set<CPGroupId> liveKeys = liveCounts.keySet();
        Set<CPGroupId> destroyedKeys = destroyedValueCounts.keySet();

        for (var e : destroyedValueCounts.entrySet()) {
            addDestroyedMetric(descriptor, context, e.getKey(), e.getValue());

            if (!liveKeys.contains(e.getKey())) {
                addLiveMetric(descriptor, context, e.getKey(), 0L);
            }
        }

        for (var e : liveCounts.entrySet()) {
            if (!destroyedKeys.contains(e.getKey())) {
                addDestroyedMetric(descriptor, context, e.getKey(), 0L);
            }
            addLiveMetric(descriptor, context, e.getKey(), e.getValue());
        }
    }

    private static void addDestroyedMetric(MetricDescriptor descriptor,
                                           MetricsCollectionContext context,
                                           CPGroupId cpGroup,
                                           long value) {
        MetricDescriptor destroyed = descriptor.copy()
                .withDiscriminator(CP_TAG_GROUP, cpGroup.getName())
                .withMetric(CP_METRIC_SUMMARY_DESTROYED_COUNT);
        context.collect(destroyed, value);
    }

    private static void addLiveMetric(MetricDescriptor descriptor,
                                      MetricsCollectionContext context,
                                      CPGroupId cpGroup,
                                      long value) {
        MetricDescriptor live = descriptor.copy()
                .withDiscriminator(CP_TAG_GROUP, cpGroup.getName())
                .withMetric(CP_METRIC_SUMMARY_LIVE_COUNT);
        context.collect(live, value);
    }

    public Collection<String> listResourceNames(CPGroupId groupId, boolean returnTombstone) {
        if (returnTombstone) {
            Set<String> destroyed = new HashSet<>();
            for (BiTuple<CPGroupId, String> tuple : destroyedValues) {
                if (groupId.equals(tuple.element1)) {
                    destroyed.add(tuple.element2);
                }
            }

            return destroyed;
        }

        Set<String> liveObjectNames = new HashSet<>();
        for (V value : atomicValues.values()) {
            if (value.groupId().equals(groupId)) {
                liveObjectNames.add(value.name());
            }
        }
        return liveObjectNames;
    }
}
