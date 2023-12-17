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

package com.hazelcast.internal.config;

import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.config.NamespacesConfig;

import javax.annotation.Nullable;

public class NamespacesConfigReadOnly extends NamespacesConfig {

    public NamespacesConfigReadOnly(NamespacesConfig config) {
        super(config);
    }

    @Override
    public NamespacesConfig setEnabled(boolean enabled) {
        throw new UnsupportedOperationException("This config is read-only name-spaces");
    }

    @Override
    public NamespacesConfig addNamespaceConfig(NamespaceConfig namespaceConfig) {
        throw new UnsupportedOperationException("This config is read-only name-spaces");
    }

    @Override
    public NamespacesConfig removeNamespaceConfig(String namespace) {
        throw new UnsupportedOperationException("This config is read-only name-spaces");
    }

    @Override
    public void setClassFilterConfig(@Nullable JavaSerializationFilterConfig classFilterConfig) {
        throw new UnsupportedOperationException("This config is read-only name-spaces");
    }
}
