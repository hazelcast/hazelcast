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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;

import java.io.IOException;

/**
 * Contains the configuration for an {@link java.util.concurrent.atomic.AtomicReference}.
 *
 * @since 3.10
 */
public class AtomicReferenceConfig extends AbstractBasicConfig<AtomicReferenceConfig> {

    private transient AtomicReferenceConfigReadOnly readOnly;

    AtomicReferenceConfig() {
    }

    public AtomicReferenceConfig(String name) {
        super(name);
    }

    public AtomicReferenceConfig(AtomicReferenceConfig config) {
        super(config);
    }

    @Override
    public Class getProvidedMergeTypes() {
        return SplitBrainMergeTypes.AtomicReferenceMergeTypes.class;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.ATOMIC_REFERENCE_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(quorumName);
        out.writeObject(mergePolicyConfig);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        quorumName = in.readUTF();
        mergePolicyConfig = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AtomicReferenceConfig)) {
            return false;
        }
        AtomicReferenceConfig that = (AtomicReferenceConfig) o;
        if (!name.equals(that.name)) {
            return false;
        }
        if (!mergePolicyConfig.equals(that.mergePolicyConfig)) {
            return false;
        }
        return quorumName != null ? quorumName.equals(that.quorumName) : that.quorumName == null;

    }

    @Override
    public final int hashCode() {
        int result = name.hashCode();
        result = 31 * result + mergePolicyConfig.hashCode();
        result = 31 * result + (quorumName != null ? quorumName.hashCode() : 0);
        return result;
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    @Override
    public AtomicReferenceConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new AtomicReferenceConfigReadOnly(this);
        }
        return readOnly;
    }

    static class AtomicReferenceConfigReadOnly extends AtomicReferenceConfig {

        AtomicReferenceConfigReadOnly(AtomicReferenceConfig config) {
            super(config);
        }

        @Override
        public AtomicReferenceConfig setName(String name) {
            throw new UnsupportedOperationException("This is a read-only config!");
        }

        @Override
        public AtomicReferenceConfig setQuorumName(String quorumName) {
            throw new UnsupportedOperationException("This is a read-only config!");
        }

        @Override
        public AtomicReferenceConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
            throw new UnsupportedOperationException("This is a read-only config!");
        }
    }
}
