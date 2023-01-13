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
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddExternalDataStoreConfigCodec;
import com.hazelcast.client.impl.protocol.util.PropertiesUtil;
import com.hazelcast.config.ExternalDataStoreConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class AddExternalDataStoreConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddExternalDataStoreConfigCodec.RequestParameters> {

    public AddExternalDataStoreConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddExternalDataStoreConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddExternalDataStoreConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddExternalDataStoreConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        ExternalDataStoreConfig config = new ExternalDataStoreConfig(parameters.name);
        config.setClassName(parameters.className);
        config.setShared(parameters.shared);
        config.setProperties(PropertiesUtil.fromMap(parameters.properties));
        return config;
    }

    @Override
    public String getMethodName() {
        return "addExternalDataStoreConfig";
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        DynamicConfigurationAwareConfig nodeConfig = (DynamicConfigurationAwareConfig) nodeEngine.getConfig();
        ExternalDataStoreConfig externalDataStoreConfig = (ExternalDataStoreConfig) config;
        return nodeConfig.checkStaticConfigDoesNotExist(nodeConfig.getStaticConfig().getExternalDataStoreConfigs(),
                externalDataStoreConfig.getName(), externalDataStoreConfig);
    }
}
