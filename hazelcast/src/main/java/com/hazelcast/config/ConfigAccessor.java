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

package com.hazelcast.config;

import com.hazelcast.client.impl.protocol.task.dynamicconfig.ResourceDefinitionHolder;
import com.hazelcast.internal.config.ServicesConfig;
import com.hazelcast.internal.namespace.ResourceDefinition;
import com.hazelcast.internal.namespace.impl.ResourceDefinitionImpl;
import com.hazelcast.spi.annotation.PrivateApi;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Map;

/**
 * Private API for accessing configuration at runtime
 *
 * @since 3.12
 */
@PrivateApi
public final class ConfigAccessor {

    private ConfigAccessor() {
    }

    public static NetworkConfig getActiveMemberNetworkConfig(Config config) {
        if (config.getAdvancedNetworkConfig().isEnabled()) {
            return new AdvancedNetworkConfig.MemberNetworkingView(config.getAdvancedNetworkConfig());
        }

        return config.getNetworkConfig();
    }

    public static ServicesConfig getServicesConfig(Config config) {
        return config.getServicesConfig();
    }

    public static boolean isInstanceTrackingEnabledSet(Config config) {
        return config.getInstanceTrackingConfig().isEnabledSet;
    }

    public static Map<String, UserCodeNamespaceConfig> getNamespaceConfigs(UserCodeNamespacesConfig config) {
       return config.getNamespaceConfigs();
    }

    public static void add(UserCodeNamespaceConfig config, @Nonnull ResourceDefinitionHolder holder) {
        config.add(new ResourceDefinitionImpl(holder));
    }

    public static Collection<ResourceDefinition> getResourceDefinitions(UserCodeNamespaceConfig nsConfig) {
        return nsConfig.getResourceConfigs();
    }

    /**
     * Adds Namespace directly to namespaces configuration without
     * Broadcasting to cluster members.
     * <p>
     * @param namespacesConfig The namespaces configuration.
     * @param namespaceConfig The namespace to add.
     */
    public static void addNamespaceConfigLocally(UserCodeNamespacesConfig namespacesConfig,
                                                 UserCodeNamespaceConfig namespaceConfig) {
        namespacesConfig.addNamespaceConfigLocally(namespaceConfig);
    }
}
