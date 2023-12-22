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
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddUserCodeNamespaceConfigCodec;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.UserCodeNamespaceConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.UserCodeNamespacePermission;

import java.security.Permission;

public class AddUserCodeNamespaceConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddUserCodeNamespaceConfigCodec.RequestParameters> {

    public AddUserCodeNamespaceConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddUserCodeNamespaceConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddUserCodeNamespaceConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddUserCodeNamespaceConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        UserCodeNamespaceConfig config = new UserCodeNamespaceConfig(parameters.name);
        parameters.resources.forEach(holder -> ConfigAccessor.add(config, holder));
        return config;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_NAMESPACE_CONFIG;
    }

    @Override
    public Permission getUserCodeNamespacePermission() {
        return parameters.name != null ? new UserCodeNamespacePermission(parameters.name, ActionConstants.ACTION_CREATE) : null;
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        // Support add-or-replace semantics
        return true;
    }
}
