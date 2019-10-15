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

package com.hazelcast.wan.impl.merkletree;

import java.util.function.Consumer;

/**
 * Interface defining methods for Merkle tree implementations
 * <p>
 * A Merkle tree is a perfect binary tree in which every leaf node holds
 * the hash of a data block and every non-leaf node holds the hash of
 * its children nodes.
 * <p>
 * In WAN replication Merkle trees are used for building a difference based
 * anti-entropy mechanism in which the source and the target clusters
 * identify the data blocks that need to be transferred from the source
 * to the target, aiming to reduce the network traffic and the time required
 * for getting the clusters in sync.
 * <p>
 * Since the Merkle trees may be updated concurrently on the clusters the
 * order of the updates (addition, removal and amendment of the entries
 * in the underlying data structure) may be different. This limits the
 * applicable hash functions to the ones that are associative. This
 * limitation ensures that the same set of entries added in any arbitrary
 * order will result the same hash. Using a non-associative hash function
 * may lead to different hashes calculated in the different clusters if
 * the entries are added in different order, which comes with unnecessary
 * data synchronization.
 */
public interface MerkleTree extends MerkleTreeView {
    /**
     * Updating the tree with adding a new entry to the tree
     *
     * @param key   The key that value belongs to
     * @param value The value of the added entry
     */
    void updateAdd(Object key, Object value);

    /**
     * Updating the tree with replacing an old value with a new one
     *
     * @param key      The key that values belong to
     * @param oldValue The old value of the replaced entry
     * @param newValue The new value of the replaced entry
     */
    void updateReplace(Object key, Object oldValue, Object newValue);

    /**
     * Updating the tree with removing an entry
     *
     * @param key          The key that values belong to
     * @param removedValue The value of the entry removed from the
     *                     underlying data structure
     */
    void updateRemove(Object key, Object removedValue);

    /**
     * Performs the given action for each key of the specified node
     * until all elements have been processed or the action throws an
     * exception.
     * <p>
     * The node referenced by {@code nodeOrder} can be either a leaf or a
     * non-leaf node. In the latter case {@code consumer} is called for
     * each key that belong the the leaves of the subtree under the
     * specified node.
     * <p>
     * This method does not define ordering for the keys belong to the
     * node {@code nodeOrder}, therefore the caller should not rely on it.
     *
     * @param nodeOrder The node for which all keys
     * @param consumer  The action which is called for each key
     */
    void forEachKeyOfNode(int nodeOrder, Consumer<Object> consumer);

    /**
     * Returns the number of the keys under the specified node.
     * <p>
     * The node referenced by {@code nodeOrder} can be either a leaf or a
     * non-leaf node. In the latter case the method returns with the sum
     * of the key counts of the subtree under the specified node
     *
     * @param nodeOrder The node for which the key count is queried
     * @return the number of the keys under the specified node
     */
    int getNodeKeyCount(int nodeOrder);

    /**
     * Returns the memory footprint of Merkle Tree in bytes
     *
     * @return the memory footprint of the Merkle Tree in bytes
     */
    long footprint();

    /**
     * Clears the Merkle tree
     */
    void clear();

}
