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

package com.hazelcast.map.impl.wan;

import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanSyncConfig;
import com.hazelcast.jet.impl.util.ConcurrentMemoizingSupplier;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.wan.impl.DelegatingWanScheme;
import com.hazelcast.wan.impl.WanReplicationService;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.internal.config.MergePolicyValidator.checkMapMergePolicy;
import static java.lang.Boolean.TRUE;

public class MapWanContext {

    protected volatile SplitBrainMergePolicy wanMergePolicy;
    protected volatile ConcurrentMemoizingSupplier<DelegatingWanScheme> wanReplicationDelegateSupplier;
    private final MapConfig mapConfig;
    private final String name;
    private final MapServiceContext mapServiceContext;
    private volatile boolean persistWanReplicatedData;

    public MapWanContext(MapContainer mapContainer) {
        this.mapConfig = mapContainer.getMapConfig();
        this.name = mapContainer.getName();
        this.mapServiceContext = mapContainer.getMapServiceContext();
    }

    public void start() {
        initWanReplication();
    }

    private void initWanReplication() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        WanReplicationRef wanReplicationRef = mapConfig.getWanReplicationRef();
        if (wanReplicationRef == null) {
            return;
        }
        String wanReplicationRefName = wanReplicationRef.getName();

        Config config = nodeEngine.getConfig();
        if (!TRUE.equals(mapConfig.getMerkleTreeConfig().getEnabled())
                && hasPublisherWithMerkleTreeSync(config, wanReplicationRefName)) {
            throw new InvalidConfigurationException(
                    "Map " + name + " has disabled merkle trees but the WAN replication scheme "
                            + wanReplicationRefName + " has publishers that use merkle trees."
                            + " Please enable merkle trees for the map.");
        }

        WanReplicationService wanReplicationService = nodeEngine.getWanReplicationService();
        // reset due to possible reconfiguration
        wanReplicationDelegateSupplier = null;
        if (wanReplicationService.hasWanReplicationScheme(wanReplicationRefName)) {
            wanReplicationDelegateSupplier = new ConcurrentMemoizingSupplier<>(() ->
                    wanReplicationService.getWanReplicationPublishers(wanReplicationRefName)
            );
        }
        SplitBrainMergePolicyProvider mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        wanMergePolicy = mergePolicyProvider.getMergePolicy(wanReplicationRef.getMergePolicyClassName());
        checkMapMergePolicy(mapConfig, wanReplicationRef.getMergePolicyClassName(), mergePolicyProvider);

        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(wanReplicationRefName);
        if (wanReplicationConfig != null) {
            WanConsumerConfig wanConsumerConfig = wanReplicationConfig.getConsumerConfig();
            if (wanConsumerConfig != null) {
                persistWanReplicatedData = wanConsumerConfig.isPersistWanReplicatedData();
            }
        }
    }

    /**
     * Returns {@code true} if at least one of the WAN publishers has
     * Merkle tree consistency check configured for the given WAN
     * replication configuration
     *
     * @param config                configuration
     * @param wanReplicationRefName The name of the WAN replication
     * @return {@code true} if there is at least one publisher has Merkle
     * tree configured
     */
    private boolean hasPublisherWithMerkleTreeSync(Config config, String wanReplicationRefName) {
        WanReplicationConfig replicationConfig = config.getWanReplicationConfig(wanReplicationRefName);
        if (replicationConfig == null) {
            return false;
        }
        return replicationConfig.getBatchPublisherConfigs()
                .stream()
                .anyMatch(c -> {
                    WanSyncConfig syncConfig = c.getSyncConfig();
                    return syncConfig != null && MERKLE_TREES.equals(syncConfig.getConsistencyCheckStrategy());
                });
    }

    public DelegatingWanScheme getWanReplicationDelegate() {
        if (wanReplicationDelegateSupplier == null) {
            return null;
        }

        return wanReplicationDelegateSupplier.get();
    }

    public SplitBrainMergePolicy getWanMergePolicy() {
        return wanMergePolicy;
    }

    public boolean isWanReplicationEnabled() {
        return wanReplicationDelegateSupplier != null && wanMergePolicy != null;
    }

    public boolean isWanRepublishingEnabled() {
        return isWanReplicationEnabled() && mapConfig.getWanReplicationRef().isRepublishingEnabled();
    }

    public boolean isPersistWanReplicatedData() {
        return persistWanReplicatedData;
    }
}
