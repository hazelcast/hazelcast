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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddQueueConfigCodec;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.List;

public class AddQueueConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddQueueConfigCodec.RequestParameters> {

    public AddQueueConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddQueueConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddQueueConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddQueueConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        QueueConfig config = new QueueConfig(parameters.name);
        config.setAsyncBackupCount(parameters.asyncBackupCount);
        config.setBackupCount(parameters.backupCount);
        config.setEmptyQueueTtl(parameters.emptyQueueTtl);
        config.setMaxSize(parameters.maxSize);
        config.setSplitBrainProtectionName(parameters.splitBrainProtectionName);
        config.setStatisticsEnabled(parameters.statisticsEnabled);
        if (parameters.queueStoreConfig != null) {
            QueueStoreConfig storeConfig = parameters.queueStoreConfig.asQueueStoreConfig(serializationService);
            config.setQueueStoreConfig(storeConfig);
        }
        if (parameters.listenerConfigs != null && !parameters.listenerConfigs.isEmpty()) {
            List<ItemListenerConfig> itemListenerConfigs =
                    (List<ItemListenerConfig>) adaptListenerConfigs(parameters.listenerConfigs);
            config.setItemListenerConfigs(itemListenerConfigs);
        }
        MergePolicyConfig mergePolicyConfig = mergePolicyConfig(parameters.mergePolicy, parameters.mergeBatchSize);
        config.setMergePolicyConfig(mergePolicyConfig);
        if (parameters.isPriorityComparatorClassNameExists) {
            config.setPriorityComparatorClassName(parameters.priorityComparatorClassName);
        }
        return config;
    }

    @Override
    public String getMethodName() {
        return "addQueueConfig";
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        DynamicConfigurationAwareConfig nodeConfig = (DynamicConfigurationAwareConfig) nodeEngine.getConfig();
        QueueConfig queueConfig = (QueueConfig) config;
        return nodeConfig.checkStaticConfigDoesNotExist(nodeConfig.getStaticConfig().getQueueConfigs(),
                queueConfig.getName(), queueConfig);
    }
}
