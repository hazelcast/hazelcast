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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.config.NamespacesConfig;

import javax.annotation.Nullable;
import java.util.function.Supplier;

/** {@link NamespacesConfig} wrapper that supports dynamically updating */
public class DynamicNamespacesConfig extends NamespacesConfig {
    /** The configuration service can change at runtime, so we always need to grab a fresh copy */
    private final Supplier<ConfigurationService> configurationServiceAccessor;

    public DynamicNamespacesConfig(Supplier<ConfigurationService> configurationServiceAccessor,
            NamespacesConfig namespacesConfig) {
        super(namespacesConfig);
        this.configurationServiceAccessor = configurationServiceAccessor;
    }

    @Override
    public NamespacesConfig addNamespaceConfig(NamespaceConfig namespaceConfig) {
        super.addNamespaceConfig(namespaceConfig);
        configurationServiceAccessor.get().broadcastConfig(namespaceConfig);
        return this;
    }

    @Override
    public NamespacesConfig removeNamespaceConfig(String namespace) {
        super.removeNamespaceConfig(namespace);
        configurationServiceAccessor.get().unbroadcastConfig(new NamespaceConfig(namespace));
        return this;
    }

    @Override
    public void setJavaSerializationFilterConfig(@Nullable JavaSerializationFilterConfig javaSerializationFilterConfig) {
        throw new UnsupportedOperationException("Cannot define JavaSerializationFilterConfig for NamespacesConfig at runtime.");
    }

    @Override
    public NamespacesConfig setEnabled(boolean enabled) {
        throw new UnsupportedOperationException("Cannot enable or disable NamespacesConfig at runtime.");
    }
}
