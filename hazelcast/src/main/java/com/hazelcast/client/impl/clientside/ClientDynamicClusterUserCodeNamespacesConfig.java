/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddUserCodeNamespaceConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigRemoveUserCodeNamespaceConfigCodec;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ResourceDefinitionHolder;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.UserCodeNamespaceConfig;
import com.hazelcast.config.UserCodeNamespacesConfig;
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.List;
import java.util.stream.Collectors;

public class ClientDynamicClusterUserCodeNamespacesConfig extends UserCodeNamespacesConfig {
    private ClientDynamicClusterConfig parent;

    public ClientDynamicClusterUserCodeNamespacesConfig(ClientDynamicClusterConfig parent) {
        this.parent = parent;
    }

    @Override
    public boolean isEnabled() {
        throw new UnsupportedOperationException(ClientDynamicClusterConfig.UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public UserCodeNamespacesConfig setEnabled(boolean enabled) {
        throw new UnsupportedOperationException(ClientDynamicClusterConfig.UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public UserCodeNamespacesConfig addNamespaceConfig(UserCodeNamespaceConfig userCodeNamespaceConfig) {
        parent.invoke(DynamicConfigAddUserCodeNamespaceConfigCodec.encodeRequest(userCodeNamespaceConfig.getName(),
                toResourceDefinitionHolders(userCodeNamespaceConfig)));
        return this;
    }

    // Exists to provide (Cloud) Operators a method of registering Namespace resources available from a URL to all members
    @PrivateApi
    public UserCodeNamespacesConfig addMemberRelativeNamespaceConfig(String namespaceName,
                                                                     List<ResourceDefinitionHolder> resourceDefinitionHolders) {
        parent.invoke(DynamicConfigAddUserCodeNamespaceConfigCodec.encodeRequest(namespaceName, resourceDefinitionHolders));
        return this;
    }

    @Override
    public UserCodeNamespacesConfig removeNamespaceConfig(String namespace) {
        parent.invoke(DynamicConfigRemoveUserCodeNamespaceConfigCodec.encodeRequest(namespace));
        return this;
    }

    private static List<ResourceDefinitionHolder> toResourceDefinitionHolders(UserCodeNamespaceConfig userCodeNamespaceConfig) {
        return ConfigAccessor.getResourceDefinitions(userCodeNamespaceConfig)
                .stream().map(ResourceDefinitionHolder::new).collect(Collectors.toList());
    }
}
