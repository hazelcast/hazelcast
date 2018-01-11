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
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddReliableIdGeneratorConfigCodec;
import com.hazelcast.config.ReliableIdGeneratorConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class AddReliableIdGeneratorConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddReliableIdGeneratorConfigCodec.RequestParameters> {

    public AddReliableIdGeneratorConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddReliableIdGeneratorConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddReliableIdGeneratorConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddReliableIdGeneratorConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        ReliableIdGeneratorConfig config = new ReliableIdGeneratorConfig(parameters.name);
        config.setPrefetchCount(parameters.prefetchCount);
        config.setPrefetchValidityMillis(parameters.prefetchValidity);
        return config;
    }

    @Override
    public String getMethodName() {
        return "addReliableIdGeneratorConfig";
    }
}
