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
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddTopicConfigCodec;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.List;

public class AddTopicConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddTopicConfigCodec.RequestParameters> {

    public AddTopicConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddTopicConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddTopicConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddTopicConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        TopicConfig config = new TopicConfig(parameters.name);
        config.setGlobalOrderingEnabled(parameters.globalOrderingEnabled);
        config.setMultiThreadingEnabled(parameters.multiThreadingEnabled);
        config.setStatisticsEnabled(parameters.statisticsEnabled);
        if (parameters.listenerConfigs != null && !parameters.listenerConfigs.isEmpty()) {
            config.setMessageListenerConfigs(
                    (List<ListenerConfig>) adaptListenerConfigs(parameters.listenerConfigs));
        }
        return config;
    }

    @Override
    public String getMethodName() {
        return "addTopicConfig";
    }
}
