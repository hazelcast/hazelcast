/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.util.StringUtil;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNull;

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
public class MerkleTreeConfig implements IdentifiedDataSerializable {
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

    private boolean enabled = true;
    private int depth = DEFAULT_DEPTH;
    private String mapName;

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
        this.mapName = config.mapName;
        this.depth = config.depth;
    }

    @Override
    public String toString() {
        return "MerkleTreeConfig{"
                + "enabled=" + enabled
                + ", depth=" + depth
                + ", mapName='" + mapName + '\''
                + '}';
    }

    /**
     * Returns an immutable version of this configuration.
     *
     * @return immutable version of this configuration
     */
    MerkleTreeConfig getAsReadOnly() {
        return new MerkleTreeConfigReadOnly(this);
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
     * @throws ConfigurationException if the {@code depth} is greater than
     *                                {@value MAX_DEPTH} or less than {@value MIN_DEPTH}
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
     */
    public boolean isEnabled() {
        return enabled;
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

    /**
     * Returns the map name to which this config applies.
     *
     * @return the map name
     */
    public String getMapName() {
        return mapName;
    }

    /**
     * Sets the map name to which this config applies. Map names
     * are also matched by pattern and merkle with map name "default"
     * applies to all maps that do not have more specific merkle tree configs.
     *
     * @param mapName the map name
     * @return the merkle tree config
     * @throws IllegalArgumentException if the {@code mapName} is {@code null} or empty.
     */
    public MerkleTreeConfig setMapName(String mapName) {
        if (StringUtil.isNullOrEmpty(mapName)) {
            throw new IllegalArgumentException("Merkle tree map name must not be empty.");
        }
        this.mapName = mapName;
        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.MERKLE_TREE_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeBoolean(enabled);
        out.writeInt(depth);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        enabled = in.readBoolean();
        depth = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MerkleTreeConfig that = (MerkleTreeConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (depth != that.depth) {
            return false;
        }
        return mapName != null ? mapName.equals(that.mapName) : that.mapName == null;
    }

    @Override
    public int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + depth;
        result = 31 * result + (mapName != null ? mapName.hashCode() : 0);
        return result;
    }

    // not private for testing
    static class MerkleTreeConfigReadOnly extends MerkleTreeConfig {
        MerkleTreeConfigReadOnly(MerkleTreeConfig config) {
            super(config);
        }

        @Override
        public MerkleTreeConfig setDepth(int depth) {
            throw new UnsupportedOperationException("This config is read-only");
        }

        @Override
        public MerkleTreeConfig setEnabled(boolean enabled) {
            throw new UnsupportedOperationException("This config is read-only");
        }

        @Override
        public MerkleTreeConfig setMapName(String mapName) {
            throw new UnsupportedOperationException("This config is read-only");
        }
    }
}
