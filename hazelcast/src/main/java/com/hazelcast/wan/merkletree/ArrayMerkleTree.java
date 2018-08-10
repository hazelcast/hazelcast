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

package com.hazelcast.wan.merkletree;

import com.hazelcast.util.collection.OAHashSet;
import com.hazelcast.util.function.Consumer;

import java.util.Arrays;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.util.JVMUtil.REFERENCE_COST_IN_BYTES;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * A not thread-safe, array based {@link MerkleTree} implementation, which
 * represents each node of the tree as a single {@code int} containing
 * the node's hash.
 * <p>
 * The array representation of the tree comes with optimal memory footprint
 * and memory layout. The memory layout is leveraged by the breadth-first
 * (and breadth-last) traversal of the tree such as recalculating the
 * hash for each non-leaf node. This traversal is optimal, since it is
 * a sequential memory walk coming with low CPU cache miss rates, which
 * in the case of a deep tree enables much better performance compared
 * to the linked representation of the tree.
 * <p>
 * The nodes in this implementation are referenced by their breadth-first
 * order in the tree. Using this order as reference comes with the
 * following properties:
 * <ul>
 * <li>The order of a node is unique for each node and refers to the node
 * on the same level and position in every binary tree. This makes a single
 * node easily referable even between clusters.
 * <li>The order of a node intrinsically encodes the level of the tree on
 * which the given node is. See {@link MerkleTreeUtil#getLevelOfNode(int)}
 * <li>The order of a node also intrinsically encodes the hash range that
 * the node covers since the followings are known based on the order:
 * <ol>
 * <li>the level the node is on
 * <li>the number of the nodes on the same level
 * <li>the node's relative order from the leftmost node on the same
 * level
 * <li>because of the points above and that the codomain of the hash
 * function is known (32 bit ints), the range of the hashes that a single
 * node on the given level covers is known, hence the range covered by a
 * concrete node can be calculated.
 * See {@link MerkleTreeUtil#getNodeHashRangeOnLevel(int)},
 * {@link MerkleTreeUtil#getNodeRangeLow(int)},
 * {@link MerkleTreeUtil#getNodeRangeHigh(int)}
 * </ol>
 * <li>The offset of a node can be calculated easily and cheaply with
 * as {@code offset = order * NODE_SIZE}.
 * </ul>
 * <p>
 * Every leaf of the tree may cover multiple elements in the underlying
 * data structure. These elements are key-value pairs and the keys of the
 * elements are stored in a data structure that are referred to as data
 * blocks. This implementation stores the keys in {@link OAHashSet}s, one
 * set for each leaf. These sets are initialized in tree construction time
 * eagerly with initial capacity calculated as
 * {@code initialCapacity = expectedEntryCount / leaves} assuming an equal
 * distribution between the leaves.
 * See {@link #ArrayMerkleTree(int, int)} and {@link #DEFAULT_EXPECTED_ENTRY_COUNT}
 * <p>
 * This implementation updates the tree synchronously. It can do that,
 * since updating a leaf's hash doesn't need to access the values belong
 * to the leaf, thank to the not order-sensitive hash function in use.
 * See {@link MerkleTreeUtil#addHash(int, int)},
 * {@link MerkleTreeUtil#removeHash(int, int)},
 * {@link MerkleTreeUtil#sumHash(int, int)}
 */
public class ArrayMerkleTree extends AbstractMerkleTreeView implements MerkleTree {
    private static final int DEFAULT_EXPECTED_ENTRY_COUNT = 1000;

    private final OAHashSet<Object>[] leafKeys;
    private final int leafLevel;
    private volatile long footprint;

    public ArrayMerkleTree(int depth) {
        this(depth, DEFAULT_EXPECTED_ENTRY_COUNT);
    }

    @SuppressWarnings("unchecked")
    public ArrayMerkleTree(int depth, int expectedEntryCount) {
        super(depth);

        checkPositive(expectedEntryCount, "Expected entry count should be a positive integer");

        this.leafLevel = depth - 1;

        final int leaves = MerkleTreeUtil.getNodesOnLevel(leafLevel);
        final int leafKeysSetCapacity = expectedEntryCount / leaves;

        leafKeys = new OAHashSet[leaves];
        for (int i = 0; i < leaves; i++) {
            leafKeys[i] = new OAHashSet<Object>(leafKeysSetCapacity);
        }
        updateFootprint();
    }

    @Override
    public void updateAdd(Object key, Object value) {
        int keyHash = key.hashCode();
        int valueHash = value.hashCode();

        int leafOrder = MerkleTreeUtil.getLeafOrderForHash(keyHash, leafLevel);
        int leafCurrentHash = getNodeHash(leafOrder);
        int leafNewHash = MerkleTreeUtil.addHash(leafCurrentHash, valueHash);

        setNodeHash(leafOrder, leafNewHash);
        addKeyToLeaf(leafOrder, keyHash, key);
        updateBranch(leafOrder);
    }

    @Override
    public void updateReplace(Object key, Object oldValue, Object newValue) {
        int keyHash = key.hashCode();
        int oldValueHash = oldValue.hashCode();
        int newValueHash = newValue.hashCode();

        int leafOrder = MerkleTreeUtil.getLeafOrderForHash(keyHash, leafLevel);
        int leafCurrentHash = getNodeHash(leafOrder);
        int leafNewHash = MerkleTreeUtil.removeHash(leafCurrentHash, oldValueHash);
        leafNewHash = MerkleTreeUtil.addHash(leafNewHash, newValueHash);

        setNodeHash(leafOrder, leafNewHash);
        updateBranch(leafOrder);
    }

    @Override
    public void updateRemove(Object key, Object removedValue) {
        int keyHash = key.hashCode();
        int removedValueHash = removedValue.hashCode();

        int leafOrder = MerkleTreeUtil.getLeafOrderForHash(keyHash, leafLevel);
        int leafCurrentHash = getNodeHash(leafOrder);
        int leafNewHash = MerkleTreeUtil.removeHash(leafCurrentHash, removedValueHash);

        setNodeHash(leafOrder, leafNewHash);
        removeKeyFromLeaf(leafOrder, keyHash, key);
        updateBranch(leafOrder);
    }

    @Override
    public int getNodeHash(int nodeOrder) {
        return tree[nodeOrder];
    }

    @Override
    public void forEachKeyOfNode(int nodeOrder, Consumer<Object> consumer) {
        if (MerkleTreeUtil.isLeaf(nodeOrder, depth)) {
            forEachKeyOfLeaf(nodeOrder, consumer);
        } else {
            forEachKeyOfNonLeaf(nodeOrder, consumer);
        }
    }

    private void forEachKeyOfLeaf(int nodeOrder, Consumer<Object> consumer) {
        int relativeLeafOrder = nodeOrder - leafLevelOrder;
        for (Object key : leafKeys[relativeLeafOrder]) {
            consumer.accept(key);
        }
    }

    private void forEachKeyOfNonLeaf(int nodeOrder, Consumer<Object> consumer) {
        final int leftMostLeaf = MerkleTreeUtil.getLeftMostLeafUnderNode(nodeOrder, depth);
        final int rightMostLeaf = MerkleTreeUtil.getRightMostLeafUnderNode(nodeOrder, depth);

        for (int leafOrder = leftMostLeaf; leafOrder <= rightMostLeaf; leafOrder++) {
            int relativeLeafOrder = leafOrder - leafLevelOrder;
            for (Object key : leafKeys[relativeLeafOrder]) {
                consumer.accept(key);
            }
        }
    }

    @Override
    public long footprint() {
        return footprint;
    }

    @Override
    public void clear() {
        Arrays.fill(tree, 0);
        for (OAHashSet<Object> leafKeys : this.leafKeys) {
            leafKeys.clear();
        }
    }

    /**
     * Synchronously calculates the hash for all the parent nodes of the
     * node referenced by {@code leafOrder} up to the root node.
     *
     * @param leafOrder The order of node
     */
    private void updateBranch(int leafOrder) {
        int nodeOrder = MerkleTreeUtil.getParentOrder(leafOrder);

        for (int level = leafLevel; level > 0; level--) {
            int leftChildOrder = MerkleTreeUtil.getLeftChildOrder(nodeOrder);
            int rightChildOrder = MerkleTreeUtil.getRightChildOrder(nodeOrder);

            int leftChildHash = getNodeHash(leftChildOrder);
            int rightChildHash = getNodeHash(rightChildOrder);

            int newNodeHash = MerkleTreeUtil.sumHash(leftChildHash, rightChildHash);
            setNodeHash(nodeOrder, newNodeHash);

            nodeOrder = MerkleTreeUtil.getParentOrder(nodeOrder);
        }
    }

    private void addKeyToLeaf(int leafOrder, int keyHash, Object key) {
        int relativeLeafOrder = leafOrder - leafLevelOrder;
        leafKeys[relativeLeafOrder].add(key, keyHash);
        updateFootprint();
    }

    private void removeKeyFromLeaf(int leafOrder, int keyHash, Object key) {
        int relativeLeafOrder = leafOrder - leafLevelOrder;
        leafKeys[relativeLeafOrder].remove(key, keyHash);
        updateFootprint();
    }

    @SuppressWarnings("checkstyle:trailingcomment")
    private void updateFootprint() {
        int leafKeysSetsFootprint = 0;
        for (OAHashSet leafKeysSet : leafKeys) {
            leafKeysSetsFootprint += leafKeysSet.footprint();
        }

        footprint = leafKeysSetsFootprint
                + INT_SIZE_IN_BYTES * tree.length
                + REFERENCE_COST_IN_BYTES * leafKeys.length
                + REFERENCE_COST_IN_BYTES // reference to the tree
                + REFERENCE_COST_IN_BYTES // reference to leafKeys array
                + INT_SIZE_IN_BYTES // depth
                + INT_SIZE_IN_BYTES // leafLevelOrder
                + INT_SIZE_IN_BYTES // leafLevel
                + LONG_SIZE_IN_BYTES; // footprint
    }

}
