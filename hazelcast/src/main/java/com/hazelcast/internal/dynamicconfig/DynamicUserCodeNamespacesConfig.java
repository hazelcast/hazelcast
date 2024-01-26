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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.config.UserCodeNamespaceConfig;
import com.hazelcast.config.UserCodeNamespacesConfig;

import javax.annotation.Nullable;
import java.util.function.Supplier;

/** {@link UserCodeNamespacesConfig} wrapper that supports dynamically updating */
public class DynamicUserCodeNamespacesConfig
        extends UserCodeNamespacesConfig {
    /** The configuration service can change at runtime, so we always need to grab a fresh copy */
    private final Supplier<ConfigurationService> configurationServiceAccessor;

    public DynamicUserCodeNamespacesConfig(Supplier<ConfigurationService> configurationServiceAccessor,
                                           UserCodeNamespacesConfig userCodeNamespacesConfig) {
        super(userCodeNamespacesConfig);
        this.configurationServiceAccessor = configurationServiceAccessor;
    }

    @Override
    public UserCodeNamespacesConfig addNamespaceConfig(UserCodeNamespaceConfig userCodeNamespaceConfig) {
        if (!isEnabled()) {
            throw new UnsupportedOperationException("Cannot add namespace while Namespaces are disabled.");
        }
        super.addNamespaceConfig(userCodeNamespaceConfig);
        configurationServiceAccessor.get().broadcastConfig(userCodeNamespaceConfig);
        return this;
    }

    @Override
    public UserCodeNamespacesConfig removeNamespaceConfig(String namespace) {
        throw new UnsupportedOperationException("Namespaces cannot be removed at runtime");
    }

    @Override
    public void setClassFilterConfig(@Nullable JavaSerializationFilterConfig classFilterConfig) {
        throw new UnsupportedOperationException("Cannot define JavaSerializationFilterConfig for NamespacesConfig at runtime.");
    }

    @Override
    public UserCodeNamespacesConfig setEnabled(boolean enabled) {
        throw new UnsupportedOperationException("Cannot enable or disable NamespacesConfig at runtime.");
    }
}
