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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddSetConfigCodec;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.List;

public class AddSetConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddSetConfigCodec.RequestParameters> {

    public AddSetConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddSetConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddSetConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddSetConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        SetConfig config = new SetConfig(parameters.name);
        config.setAsyncBackupCount(parameters.asyncBackupCount);
        config.setBackupCount(parameters.backupCount);
        config.setMaxSize(parameters.maxSize);
        config.setStatisticsEnabled(parameters.statisticsEnabled);
        if (parameters.listenerConfigs != null && !parameters.listenerConfigs.isEmpty()) {
            List<ItemListenerConfig> itemListenerConfigs = (List<ItemListenerConfig>)
                    adaptListenerConfigs(parameters.listenerConfigs);
            config.setItemListenerConfigs(itemListenerConfigs);
        }
        MergePolicyConfig mergePolicyConfig = mergePolicyConfig(parameters.mergePolicyExist, parameters.mergePolicy,
                parameters.mergeBatchSize);
        config.setMergePolicyConfig(mergePolicyConfig);
        return config;
    }

    @Override
    public String getMethodName() {
        return "addSetConfig";
    }
}
