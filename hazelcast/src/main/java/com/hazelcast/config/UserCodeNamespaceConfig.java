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

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.internal.namespace.ResourceDefinition;
import com.hazelcast.internal.namespace.impl.ResourceDefinitionImpl;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.config.ResourceType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Contains the configuration for a specific User Code Namespace, defining its identifier
 * (name) as well as the definitions for resources it provides.
 *
 * @since 5.4
 */
@Beta
public class UserCodeNamespaceConfig
        implements NamedConfig, IdentifiedDataSerializable {
    private String name;

    private final Map<String, ResourceDefinition> resourceDefinitions = new ConcurrentHashMap<>();

    public UserCodeNamespaceConfig() {
    }

    public UserCodeNamespaceConfig(String name) {
        this.name = name;
    }

    public UserCodeNamespaceConfig(UserCodeNamespaceConfig config) {
        this.name = config.name;
        this.resourceDefinitions.putAll(config.resourceDefinitions);
    }

    public UserCodeNamespaceConfig(@Nonnull String name, @Nonnull Map<String, ResourceDefinition> resources) {
        this.name = name;
        this.resourceDefinitions.putAll(resources);
    }

    @Override
    public UserCodeNamespaceConfig setName(@Nonnull String name) {
        Objects.requireNonNull(name, "Namespace name cannot be null");
        this.name = name;
        return this;
    }

    @Override
    public String getName() {
        return name;
    }

    public UserCodeNamespaceConfig addClass(@Nonnull Class<?>... classes) {
        Objects.requireNonNull(classes, "Classes cannot be null");
        ResourceConfig.fromClass(classes).map(ResourceDefinitionImpl::new)
                .forEach(resourceDefinition -> resourceDefinitions.put(resourceDefinition.id(), resourceDefinition));
        return this;
    }

    public UserCodeNamespaceConfig addJar(@Nonnull URL url, @Nullable String id) {
        return add(url, id, ResourceType.JAR);
    }

    public UserCodeNamespaceConfig addJarsInZip(@Nonnull URL url, @Nullable String id) {
        return add(url, id, ResourceType.JARS_IN_ZIP);
    }

    UserCodeNamespaceConfig add(@Nonnull URL url, @Nullable String id, @Nonnull ResourceType resourceType) {
        return add(new ResourceDefinitionImpl(new ResourceConfig(url, id, resourceType)));
    }

    protected UserCodeNamespaceConfig add(ResourceDefinition resourceDefinition) {
        if (resourceDefinitions.putIfAbsent(resourceDefinition.id(), resourceDefinition) != null) {
            throw new IllegalArgumentException("Resource with id: " + resourceDefinition.id() + " already exists");
        } else {
            return this;
        }
    }

    Set<ResourceDefinition> getResourceConfigs() {
        return Set.copyOf(resourceDefinitions.values());
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        SerializationUtil.writeMapStringKey(resourceDefinitions, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            resourceDefinitions.put(in.readString(), in.readObject());
        }
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.NAMESPACE_CONFIG;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        UserCodeNamespaceConfig other = (UserCodeNamespaceConfig) obj;
        return Objects.equals(name, other.name);
    }
}
