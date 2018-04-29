/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.merge.SplitBrainMergeTypeProvider;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Provides a basic configuration for a split-brain aware data structure.
 *
 * @param <T> type of the config
 * @since 3.10
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractBasicConfig<T extends AbstractBasicConfig>
        implements SplitBrainMergeTypeProvider, IdentifiedDataSerializable {

    protected String name;
    protected String quorumName;
    protected MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();

    protected AbstractBasicConfig() {
    }

    protected AbstractBasicConfig(String name) {
        this.name = name;
    }

    protected AbstractBasicConfig(AbstractBasicConfig config) {
        this.name = config.name;
        this.quorumName = config.quorumName;
        this.mergePolicyConfig = config.mergePolicyConfig;
    }

    abstract T getAsReadOnly();

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
     * Returns the quorum name for operations.
     *
     * @return the quorum name
     */
    public String getQuorumName() {
        return quorumName;
    }

    /**
     * Sets the quorum name for operations.
     *
     * @param quorumName the quorum name
     * @return the updated configuration
     */
    public T setQuorumName(String quorumName) {
        this.quorumName = quorumName;
        //noinspection unchecked
        return (T) this;
    }

    @Override
    public String toString() {
        return  getClass().getSimpleName() + "{"
                + "name='" + name + '\''
                + ", quorumName=" + quorumName
                + ", mergePolicyConfig=" + mergePolicyConfig
                + "}";
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }
}
