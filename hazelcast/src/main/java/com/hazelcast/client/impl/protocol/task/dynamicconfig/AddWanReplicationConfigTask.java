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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddWanReplicationConfigCodec;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AddWanReplicationConfigTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddWanReplicationConfigCodec.RequestParameters> {
    public AddWanReplicationConfigTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddWanReplicationConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddWanReplicationConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddWanReplicationConfigCodec.encodeResponse();
    }

    @Override
    public String getMethodName() {
        return "addWanReplicationConfig";
    }


    @Override
    protected IdentifiedDataSerializable getConfig() {
        WanReplicationConfigTransformer transformer = new WanReplicationConfigTransformer(serializationService);
        WanConsumerConfig wanConsumerConfig = transformer.toConfig(parameters.consumerConfig);

        List<WanCustomPublisherConfig> customPublisherConfigs =
                parameters.customPublisherConfigs.stream()
                                                 .filter(Objects::nonNull)
                                                 .map(transformer::toConfig)
                                                 .collect(Collectors.toList());
        List<WanBatchPublisherConfig> batchPublisherConfigs =
                parameters.batchPublisherConfigs.stream()
                                                 .filter(Objects::nonNull)
                                                 .map(transformer::toConfig)
                                                 .collect(Collectors.toList());

        WanReplicationConfig config = new WanReplicationConfig();
        config.setName(parameters.name);
        if (wanConsumerConfig != null) {
            config.setConsumerConfig(wanConsumerConfig);
        }
        config.setCustomPublisherConfigs(customPublisherConfigs);
        config.setBatchPublisherConfigs(batchPublisherConfigs);
        return config;
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        DynamicConfigurationAwareConfig nodeConfig = (DynamicConfigurationAwareConfig) nodeEngine.getConfig();
        WanReplicationConfig wanReplicationConfig = (WanReplicationConfig) config;
        return DynamicConfigurationAwareConfig.checkStaticConfigDoesNotExist(
                nodeConfig.getStaticConfig().getWanReplicationConfigs(),
                wanReplicationConfig.getName(),
                wanReplicationConfig);
    }
}
