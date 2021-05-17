/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanSyncConfig;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.wan.impl.DelegatingWanScheme;
import com.hazelcast.wan.impl.InternalWanEvent;
import com.hazelcast.wan.impl.WanReplicationService;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;

public final class QueueWANPublisherImpl implements QueueWANPublisher {

    private final String queueName;
    private final QueueConfig queueConfig;
    private DelegatingWanScheme wanReplicationDelegate;
    private boolean persistWanReplicatedData;
    private SplitBrainMergePolicy wanMergePolicy;

    public QueueWANPublisherImpl(String queueName, QueueConfig queueConfig) {
        this.queueName = queueName;
        this.queueConfig = queueConfig;
    }

    final QueueWANPublisher init(NodeEngine nodeEngine) {
        WanReplicationRef wanReplicationRef = queueConfig.getWanReplicationRef();
        if (wanReplicationRef == null) {
            return this;
        }
        String wanReplicationRefName = wanReplicationRef.getName();

        Config config = nodeEngine.getConfig();
        if (!queueConfig.getMerkleTreeConfig().isEnabled()
                && hasPublisherWithMerkleTreeSync(config, wanReplicationRefName)) {
            throw new InvalidConfigurationException(
                    "Queue " + queueName + " has disabled merkle trees but the WAN replication scheme "
                            + wanReplicationRefName + " has publishers that use merkle trees."
                            + " Please enable merkle trees for the queue.");
        }

        WanReplicationService wanReplicationService = nodeEngine.getWanReplicationService();
        wanReplicationDelegate = wanReplicationService.getWanReplicationPublishers(wanReplicationRefName);
        SplitBrainMergePolicyProvider mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        wanMergePolicy = mergePolicyProvider.getMergePolicy(wanReplicationRef.getMergePolicyClassName());

        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(wanReplicationRefName);
        if (wanReplicationConfig != null) {
            WanConsumerConfig wanConsumerConfig = wanReplicationConfig.getConsumerConfig();
            if (wanConsumerConfig != null) {
                persistWanReplicatedData = wanConsumerConfig.isPersistWanReplicatedData();
            }
        }

        return this;
    }

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

    @Override
    public final void publishWanEvent(InternalWanEvent event, boolean isPrimaryReplica) {
        if (isPrimaryReplica) {
            wanReplicationDelegate.publishReplicationEvent(event);
        } else {
            wanReplicationDelegate.publishReplicationEventBackup(event);
        }
    }
}
