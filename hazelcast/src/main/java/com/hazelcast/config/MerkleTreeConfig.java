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
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.annotation.PrivateApi;

import java.io.IOException;
import java.util.Objects;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableBoolean;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableBoolean;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Configuration for a merkle tree.
 * The merkle tree is a data structure used for efficient comparison of the
 * difference in the contents of large data structures. The precision of
 * such a comparison mechanism is defined by the depth of the merkle tree.
 * <p>
 * A larger depth means that a data synchronization mechanism will be able
 * to pinpoint a smaller subset of the data structure contents in which a
 * change occurred. This causes the synchronization mechanism to be more
 * efficient. On the other hand, a larger tree depth means the merkle tree
 * will consume more memory.
 * <p>
 * A smaller depth means the data synchronization mechanism will have to
 * transfer larger chunks of the data structure in which a possible change
 * happened. On the other hand, a shallower tree consumes less memory.
 * The depth must be between {@value MIN_DEPTH} and {@value MAX_DEPTH}
 * (exclusive). The default depth is {@value DEFAULT_DEPTH}.
 * <p>
 * As the comparison mechanism is iterative, a larger depth will also prolong
 * the duration of the comparison mechanism. Care must be taken to not have
 * large tree depths if the latency of the comparison operation is high.
 * The default depth is {@value DEFAULT_DEPTH}.
 * <p>
 * See https://en.wikipedia.org/wiki/Merkle_tree.
 *
 * @since 3.11
 */
public class MerkleTreeConfig implements IdentifiedDataSerializable, Versioned {
    /**
     * Minimal depth of the merkle tree.
     */
    private static final int MIN_DEPTH = 2;
    /**
     * Maximal depth of the merkle tree.
     */
    private static final int MAX_DEPTH = 27;
    /**
     * Default depth of the merkle tree.
     */
    private static final int DEFAULT_DEPTH = 10;

    private Boolean enabled;
    private int depth = DEFAULT_DEPTH;

    public MerkleTreeConfig() {
    }

    /**
     * Clones a {@link MerkleTreeConfig}.
     *
     * @param config the merkle tree config to clone
     * @throws NullPointerException if the config is null
     */
    public MerkleTreeConfig(MerkleTreeConfig config) {
        checkNotNull(config, "config can't be null");
        this.enabled = config.enabled;
        this.depth = config.depth;
    }

    @Override
    public String toString() {
        return "MerkleTreeConfig{"
                + "enabled=" + enabled
                + ", depth=" + depth
                + '}';
    }

    /**
     * Returns the depth of the merkle tree.
     * The default depth is {@value DEFAULT_DEPTH}.
     */
    public int getDepth() {
        return depth;
    }

    /**
     * Sets the depth of the merkle tree. The depth must be between
     * {@value MAX_DEPTH} and {@value MIN_DEPTH} (exclusive).
     *
     * @param depth the depth of the merkle tree
     * @return the updated config
     * @throws InvalidConfigurationException if the {@code depth} is greater than
     *                                       {@value MAX_DEPTH} or less than {@value MIN_DEPTH}
     */
    public MerkleTreeConfig setDepth(int depth) {
        if (depth < MIN_DEPTH || depth > MAX_DEPTH) {
            throw new IllegalArgumentException("Merkle tree depth " + depth + " is outside of the allowed range "
                    + MIN_DEPTH + "-" + MAX_DEPTH + ". ");
        }
        this.depth = depth;
        return this;
    }

    /**
     * Returns if the merkle tree is enabled.
     *
     * @return {@code true} if the merkle tree is enabled, {@code false} otherwise
     * @deprecated use {@link #getEnabled()} instead.
     */
    @Deprecated
    public boolean isEnabled() {
        return Boolean.TRUE.equals(enabled);
    }

    /**
     * Returns if the merkle tree is enabled.
     *
     * @return {@code TRUE} if the merkle tree is enabled, {@code FALSE} if disabled
     *          or {@code null} if user did not explicitly configure Merkle trees,
     *          so it may or may not be enabled by Hazelcast.
     */
    public Boolean getEnabled() {
        return enabled;
    }

    /**
     * Method introduced for client protocol compatibility.
     *
     * @return whether {@code enabled} field is set or not.
     */
    @PrivateApi
    public boolean isEnabledSet() {
        return enabled != null;
    }

    /**
     * Enables or disables the merkle tree.
     *
     * @param enabled {@code true} if enabled, {@code false} otherwise.
     * @return the updated config
     */
    public MerkleTreeConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.MERKLE_TREE_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        writeNullableBoolean(out, enabled);
        out.writeInt(depth);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        enabled = readNullableBoolean(in);
        depth = in.readInt();
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MerkleTreeConfig)) {
            return false;
        }

        MerkleTreeConfig that = (MerkleTreeConfig) o;

        if (!Objects.equals(enabled, that.enabled)) {
            return false;
        }
        return depth == that.depth;

    }

    @Override
    public final int hashCode() {
        int result = Objects.hashCode(enabled);
        result = 31 * result + depth;
        return result;
    }
}
