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
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddMultiMapConfigCodec;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class AddMultiMapConfigMessageTask extends
        AbstractAddConfigMessageTask<DynamicConfigAddMultiMapConfigCodec.RequestParameters> {

    public AddMultiMapConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddMultiMapConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddMultiMapConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddMultiMapConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        MultiMapConfig multiMapConfig = new MultiMapConfig();
        multiMapConfig.setName(parameters.name);
        multiMapConfig.setValueCollectionType(parameters.collectionType);
        multiMapConfig.setAsyncBackupCount(parameters.asyncBackupCount);
        multiMapConfig.setBackupCount(parameters.backupCount);
        multiMapConfig.setBinary(parameters.binary);
        multiMapConfig.setStatisticsEnabled(parameters.statisticsEnabled);
        if (parameters.listenerConfigs != null && !parameters.listenerConfigs.isEmpty()) {
            for (ListenerConfigHolder configHolder : parameters.listenerConfigs) {
                EntryListenerConfig entryListenerConfig = configHolder.asListenerConfig(serializationService);
                multiMapConfig.addEntryListenerConfig(entryListenerConfig);
            }
        }
        MergePolicyConfig mergePolicyConfig = mergePolicyConfig(parameters.mergePolicy, parameters.mergeBatchSize);
        multiMapConfig.setMergePolicyConfig(mergePolicyConfig);
        return multiMapConfig;
    }

    @Override
    public String getMethodName() {
        return "addMultiMapConfig";
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        DynamicConfigurationAwareConfig nodeConfig = (DynamicConfigurationAwareConfig) nodeEngine.getConfig();
        MultiMapConfig multiMapConfig = (MultiMapConfig) config;
        return nodeConfig.checkStaticConfigDoesNotExist(nodeConfig.getStaticConfig().getMultiMapConfigs(),
                multiMapConfig.getName(), multiMapConfig);
    }
}
