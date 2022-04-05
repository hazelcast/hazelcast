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
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddReliableTopicConfigCodec;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.topic.TopicOverloadPolicy;

import java.util.List;
import java.util.concurrent.Executor;

public class AddReliableTopicConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddReliableTopicConfigCodec.RequestParameters> {

    public AddReliableTopicConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddReliableTopicConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddReliableTopicConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddReliableTopicConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        ReliableTopicConfig config = new ReliableTopicConfig(parameters.name);
        config.setStatisticsEnabled(parameters.statisticsEnabled);
        config.setReadBatchSize(parameters.readBatchSize);
        config.setTopicOverloadPolicy(TopicOverloadPolicy.valueOf(parameters.topicOverloadPolicy));
        Executor executor = serializationService.toObject(parameters.executor);
        config.setExecutor(executor);
        if (parameters.listenerConfigs != null && !parameters.listenerConfigs.isEmpty()) {
            config.setMessageListenerConfigs(
                    (List<ListenerConfig>) adaptListenerConfigs(parameters.listenerConfigs));
        }
        return config;
    }

    @Override
    public String getMethodName() {
        return "addReliableTopicConfig";
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        DynamicConfigurationAwareConfig nodeConfig = (DynamicConfigurationAwareConfig) nodeEngine.getConfig();
        ReliableTopicConfig reliableTopicConfig = (ReliableTopicConfig) config;
        return nodeConfig.checkStaticConfigDoesNotExist(nodeConfig.getStaticConfig().getReliableTopicConfigs(),
                reliableTopicConfig.getName(), reliableTopicConfig);
    }
}
