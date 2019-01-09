/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddSemaphoreConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddSetConfigCodec;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class AddSemaphoreConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddSemaphoreConfigCodec.RequestParameters> {

    public AddSemaphoreConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddSemaphoreConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddSemaphoreConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddSetConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        SemaphoreConfig config = new SemaphoreConfig();
        config.setName(parameters.name);
        config.setBackupCount(parameters.backupCount);
        config.setAsyncBackupCount(parameters.asyncBackupCount);
        config.setInitialPermits(parameters.initialPermits);
        return config;
    }

    @Override
    public String getMethodName() {
        return "addSemaphoreConfig";
    }
}
