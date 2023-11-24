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
package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddNamespaceConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigRemoveNamespaceConfigCodec;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ResourceDefinitionHolder;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.config.NamespacesConfig;

import java.util.List;
import java.util.stream.Collectors;

public class ClientDynamicClusterNamespaceConfig extends NamespacesConfig {
    private ClientDynamicClusterConfig parent;

    public ClientDynamicClusterNamespaceConfig(ClientDynamicClusterConfig parent) {
        this.parent = parent;
    }

    @Override
    public boolean isEnabled() {
        throw new UnsupportedOperationException(ClientDynamicClusterConfig.UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public NamespacesConfig setEnabled(boolean enabled) {
        throw new UnsupportedOperationException(ClientDynamicClusterConfig.UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public NamespacesConfig addNamespaceConfig(NamespaceConfig namespaceConfig) {
        parent.invoke(DynamicConfigAddNamespaceConfigCodec.encodeRequest(namespaceConfig.getName(),
                toResourceDefinitionHolders(namespaceConfig)));
        return this;
    }

    @Override
    public NamespacesConfig removeNamespaceConfig(String namespace) {
        parent.invoke(DynamicConfigRemoveNamespaceConfigCodec.encodeRequest(namespace));
        return this;
    }

    private static List<ResourceDefinitionHolder> toResourceDefinitionHolders(NamespaceConfig namespaceConfig) {
        return ConfigAccessor.getResourceDefinitions(namespaceConfig)
                .stream().map(ResourceDefinitionHolder::new).collect(Collectors.toList());
    }
}
