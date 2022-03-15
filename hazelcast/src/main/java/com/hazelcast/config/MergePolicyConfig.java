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

package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;

import java.io.IOException;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Configuration for {@link SplitBrainMergePolicy}.
 */
public class MergePolicyConfig implements IdentifiedDataSerializable {

    /**
     * Default merge policy.
     */
    public static final String DEFAULT_MERGE_POLICY = PutIfAbsentMergePolicy.class.getName();

    /**
     * Default batch size.
     */
    public static final int DEFAULT_BATCH_SIZE = 100;

    private String policy = DEFAULT_MERGE_POLICY;
    private int batchSize = DEFAULT_BATCH_SIZE;

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

    /**
     * Returns the classname of the {@link SplitBrainMergePolicy}.
     *
     * @return the classname of the merge policy
     */
    public String getPolicy() {
        return policy;
    }

    /**
     * Sets the classname of your {@link SplitBrainMergePolicy}.
     * <p>
     * For the out-of-the-box merge policies the simple classname
     * is sufficient, e.g. {@code PutIfAbsentMergePolicy}.
     * But also the fully qualified classname is fine, e.g.
     * com.hazelcast.spi.merge.PutIfAbsentMergePolicy. For a
     * custom merge policy the fully qualified classname is needed.
     * <p>
     * Must be a non-empty string. The default
     * value is {@code PutIfAbsentMergePolicy}.
     *
     * @param policy the classname of the merge policy
     * @return this {@code MergePolicyConfig} instance
     */
    public MergePolicyConfig setPolicy(String policy) {
        this.policy = checkHasText(policy, "Merge policy must contain text!");
        return this;
    }

    /**
     * Returns the batch size, which will be used to determine
     * the number of entries to be sent in a merge operation.
     *
     * @return the batch size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the batch size, which will be used to determine
     * the number of entries to be sent in a merge operation.
     * <p>
     * Must be a positive number. Set to {@code 1} to disable
     * batching. The default value is {@value #DEFAULT_BATCH_SIZE}.
     *
     * @param batchSize the batch size
     * @return this {@code MergePolicyConfig} instance
     */
    public MergePolicyConfig setBatchSize(int batchSize) {
        this.batchSize = checkPositive("batchSize", batchSize);
        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.MERGE_POLICY_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(policy);
        out.writeInt(batchSize);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        policy = in.readString();
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
        return Objects.equals(policy, that.policy);
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
}
