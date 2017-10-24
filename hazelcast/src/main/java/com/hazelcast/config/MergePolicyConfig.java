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
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Configuration for {@link com.hazelcast.spi.SplitBrainMergePolicy}.
 */
public class MergePolicyConfig implements IdentifiedDataSerializable {

    /**
     * Default merge policy.
     */
    public static final String DEFAULT_MERGE_POLICY = PutIfAbsentMergePolicy.class.getSimpleName();

    /**
     * Default batch size.
     */
    public static final int DEFAULT_BATCH_SIZE = 100;

    protected String policy = DEFAULT_MERGE_POLICY;
    protected int batchSize = DEFAULT_BATCH_SIZE;

    protected MergePolicyConfig readOnly;

    public MergePolicyConfig() {
    }

    public MergePolicyConfig(String policy, int batchSize) {
        setPolicy(policy);
        setBatchSize(batchSize);
    }

    public MergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        this.policy = mergePolicyConfig.policy;
        this.batchSize = mergePolicyConfig.batchSize;
    }

    public String getPolicy() {
        return policy;
    }

    public MergePolicyConfig setPolicy(String policy) {
        this.policy = checkHasText(policy, "Merge policy must contain text!");
        return this;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public MergePolicyConfig setBatchSize(int batchSize) {
        this.batchSize = checkPositive(batchSize, "batchSize must be a positive number!");
        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.MERGE_POLICY_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(policy);
        out.writeInt(batchSize);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        policy = in.readUTF();
        batchSize = in.readInt();
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MergePolicyConfig)) {
            return false;
        }

        MergePolicyConfig that = (MergePolicyConfig) o;
        if (batchSize != that.batchSize) {
            return false;
        }
        return policy != null ? policy.equals(that.policy) : that.policy == null;
    }

    @Override
    public final int hashCode() {
        int result = policy != null ? policy.hashCode() : 0;
        result = 31 * result + batchSize;
        return result;
    }

    @Override
    public String toString() {
        return "MergePolicyConfig{"
                + "policy='" + policy + '\''
                + ", batchSize=" + batchSize
                + '}';
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public MergePolicyConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new MergePolicyConfigReadOnly(this);
        }
        return readOnly;
    }

    private class MergePolicyConfigReadOnly extends MergePolicyConfig {

        MergePolicyConfigReadOnly(MergePolicyConfig mergePolicyConfig) {
            super(mergePolicyConfig);
        }

        @Override
        public MergePolicyConfig setPolicy(String policy) {
            throw new UnsupportedOperationException("This is a read-only configuration");
        }

        @Override
        public MergePolicyConfig setBatchSize(int batchSize) {
            throw new UnsupportedOperationException("This is a read-only configuration");
        }
    }
}
