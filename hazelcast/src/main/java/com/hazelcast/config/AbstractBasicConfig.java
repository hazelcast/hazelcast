/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Provides a basic configuration for a split-brain aware data structure.
 *
 * @param <T> type of the config
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractBasicConfig<T extends AbstractBasicConfig> implements IdentifiedDataSerializable {

    private String name;
    private MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();

    protected AbstractBasicConfig() {
    }

    protected AbstractBasicConfig(AbstractBasicConfig config) {
        this.name = config.name;
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

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(mergePolicyConfig);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        mergePolicyConfig = in.readObject();
    }

    @Override
    @SuppressWarnings("SimplifiableIfStatement")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractBasicConfig)) {
            return false;
        }

        AbstractBasicConfig<?> that = (AbstractBasicConfig<?>) o;
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        return mergePolicyConfig != null ? mergePolicyConfig.equals(that.mergePolicyConfig) : that.mergePolicyConfig == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (mergePolicyConfig != null ? mergePolicyConfig.hashCode() : 0);
        return result;
    }

    /**
     * Returns field names with values as concatenated String so it can be used in child classes' toString() methods.
     */
    protected String fieldsToString() {
        return "name='" + name + "'"
                + ", mergePolicyConfig=" + mergePolicyConfig;
    }
}
