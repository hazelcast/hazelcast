/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.NamedConfig;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Provides a basic configuration for a split-brain aware data structure.
 *
 * @param <T> type of the config
 * @since 3.10
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractBasicConfig<T extends AbstractBasicConfig>
        implements IdentifiedDataSerializable, NamedConfig {

    protected String name;
    protected String splitBrainProtectionName;
    protected MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();

    protected AbstractBasicConfig() {
    }

    protected AbstractBasicConfig(String name) {
        this.name = name;
    }

    protected AbstractBasicConfig(AbstractBasicConfig config) {
        this.name = config.name;
        this.splitBrainProtectionName = config.splitBrainProtectionName;
        this.mergePolicyConfig = new MergePolicyConfig(config.mergePolicyConfig);
    }

    /**
     * Gets the name of this data structure.
     *
     * @return the name of this data structure
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of this data structure.
     *
     * @param name the name of this data structure
     * @return the updated configuration
     */
    public T setName(String name) {
        this.name = checkNotNull(name, "name cannot be null");
        //noinspection unchecked
        return (T) this;
    }

    /**
     * Gets the {@link MergePolicyConfig} of this data structure.
     *
     * @return the {@link MergePolicyConfig} of this data structure
     */
    public MergePolicyConfig getMergePolicyConfig() {
        return mergePolicyConfig;
    }

    /**
     * Sets the {@link MergePolicyConfig} for this data structure.
     *
     * @return the updated configuration
     */
    public T setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        this.mergePolicyConfig = checkNotNull(mergePolicyConfig, "mergePolicyConfig cannot be null");
        //noinspection unchecked
        return (T) this;
    }

    /**
     * Returns the split brain protection name for operations.
     *
     * @return the split brain protection name
     */
    public String getSplitBrainProtectionName() {
        return splitBrainProtectionName;
    }

    /**
     * Sets the split brain protection name for operations.
     *
     * @param splitBrainProtectionName the split brain protection name
     * @return the updated configuration
     */
    public T setSplitBrainProtectionName(String splitBrainProtectionName) {
        this.splitBrainProtectionName = splitBrainProtectionName;
        //noinspection unchecked
        return (T) this;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{"
                + "name='" + name + '\''
                + ", splitBrainProtectionName=" + splitBrainProtectionName
                + ", mergePolicyConfig=" + mergePolicyConfig
                + "}";
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }
}
