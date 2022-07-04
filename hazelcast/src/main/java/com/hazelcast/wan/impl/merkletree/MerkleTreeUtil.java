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

package com.hazelcast.wan.impl.merkletree;

import com.hazelcast.internal.util.QuickMath;
import com.hazelcast.internal.util.collection.IntHashSet;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * Utility methods for Merkle tree implementation
 */
public final class MerkleTreeUtil {
    static final int HUGE_PRIME = 2038079003;
    @SuppressWarnings("checkstyle:magicnumber")
    private static final long INT_RANGE = 1L << 32;

    private MerkleTreeUtil() {
    }

    /**
     * Returns the breadth-first order of the leaf that a given {@code hash}
     * belongs to
     *
     * @param hash  The hash for which the leaf order to be calculated
     * @param level The level
     * @return the breadth-first order of the leaf for the given {@code hash}
     */
    public static int getLeafOrderForHash(int hash, int level) {
        long hashStepForLevel = getNodeHashRangeOnLevel(level);
        long hashDistanceFromMin = ((long) hash) - Integer.MIN_VALUE;
        int steps = (int) (hashDistanceFromMin / hashStepForLevel);
        int leftMostNodeOrderOnLevel = getLeftMostNodeOrderOnLevel(level);

        return leftMostNodeOrderOnLevel + steps;
    }

    /**
     * Returns the hash range that one node covers on the given {@code level}
     *
     * @param level The level the node hash range to be calculated for
     * @return the hash range of a node on the given {@code level}
     */
    static long getNodeHashRangeOnLevel(int level) {
        int nodesOnLevel = getNodesOnLevel(level);
        return INT_RANGE / nodesOnLevel;
    }

    /**
     * Returns the lower bound of the hash range for a given node
     *
     * @param nodeOrder The breadth-first order of the node
     * @return the lower bound of the hash range
     */
    static int getNodeRangeLow(int nodeOrder) {
        int level = getLevelOfNode(nodeOrder);
        int leftMostLeafOrder = getLeftMostNodeOrderOnLevel(level);
        int levelHashStep = (int) getNodeHashRangeOnLevel(level);
        int leafOrderOnLevel = nodeOrder - leftMostLeafOrder;

        return Integer.MIN_VALUE + (leafOrderOnLevel * levelHashStep);
    }

    /**
     * Returns the upper bound of the hash range for a given node
     *
     * @param nodeOrder The breadth-first order of the node
     * @return the upper bound of the hash range
     */
    static int getNodeRangeHigh(int nodeOrder) {
        int level = getLevelOfNode(nodeOrder);
        int leftMostLeafOrder = getLeftMostNodeOrderOnLevel(level);
        int levelHashStep = (int) getNodeHashRangeOnLevel(level);
        int leafOrderOnLevel = nodeOrder - leftMostLeafOrder;

        return Integer.MIN_VALUE + ((leafOrderOnLevel + 1) * levelHashStep) - 1;
    }

    /**
     * Adds a hash to the accumulated hash of a tree node
     * <p>
     * Notes:
     * <ul>
     * <li>The hash function must be associative. For the detailed
     * reasoning see {@link MerkleTree}
     * <li>{@link #addHash(int, int)} and {@link #removeHash(int, int)} is
     * distinguished since the result accumulated hash must be the same if
     * an entry's hash is added and then removed and if the entry is not
     * added. This distinction acts as an optimisation, since this way
     * iterating over the entries and their hashes belong to a leaf is not
     * required when a leaf's hash is updated.
     * </ul>
     *
     * @param originalHash The accumulated hash before {@code addedHash}
     *                     is added
     * @param addedHash    The hash to be added to the accumulated hash
     * @return the accumulated hash with {@code addedHash} added
     * @see #removeHash(int, int)
     */
    static int addHash(int originalHash, int addedHash) {
        return originalHash + HUGE_PRIME * addedHash;
    }

    /**
     * Removes a hash from the accumulated hash of a tree node
     * <p>
     * Notes:
     * <ul>
     * <li>The hash function must be associative. For the detailed
     * reasoning see {@link MerkleTree}
     * <li>{@link #addHash(int, int)} and {@link #removeHash(int, int)} is
     * distinguished since the result accumulated hash must be the same if
     * an entry's hash is added and then removed and if the entry is not
     * added. This distinction acts as an optimisation, since this way
     * iterating over the entries and their hashes belong to a leaf is not
     * required when a leaf's hash is updated.
     * </ul>
     *
     * @param originalHash The accumulated hash before {@code removedHash}
     *                     is removed
     * @param removedHash  The hash to be removed from the accumulated hash
     * @return the accumulated hash with {@code removedHash} removed
     * @see #addHash(int, int)
     */
    static int removeHash(int originalHash, int removedHash) {
        return originalHash - HUGE_PRIME * removedHash;
    }

    /**
     * Sums the hash of the left and the right children
     * <p>
     * May be used only when calculating a non-leaf node's hash code from
     * its children's hashes.
     *
     * @param leftHash  The hash of the left child
     * @param rightHash The hash of the right child
     * @return the accumulated hash
     * @see #addHash(int, int)
     * @see #removeHash(int, int)
     */
    public static int sumHash(int leftHash, int rightHash) {
        return leftHash + rightHash;
    }

    /**
     * Returns the level a node with the given {@code order} is on in the tree
     *
     * @param nodeOrder The breadth-first order of a node for which its level
     *                  to be calculated
     * @return the level which the node with {@code order} is on
     */
    public static int getLevelOfNode(int nodeOrder) {
        return QuickMath.log2(nodeOrder + 1);
    }

    /**
     * Returns the breadth-first order of the leftmost node on a given {@code level}
     *
     * @param level The level for which the leftmost node's order to be
     *              calculated
     * @return the order of the leftmost node on the given level
     */
    static int getLeftMostNodeOrderOnLevel(int level) {
        return (1 << level) - 1;
    }

    /**
     * Returns the number of nodes on the provided {@code level}
     *
     * @param level The level
     * @return the number of the nodes on the given level
     */
    public static int getNodesOnLevel(int level) {
        return 1 << level;
    }

    /**
     * Returns the breadth-first order of the parent node of the node
     * with {@code nodeOrder}
     *
     * @param nodeOrder The order of the node for which its parent's order
     *                  to be calculated
     * @return the order of the parent node
     */
    public static int getParentOrder(int nodeOrder) {
        return (nodeOrder - 1) >> 1;
    }

    /**
     * Returns the breadth-first order of the left child node of the node
     * with {@code nodeOrder}
     *
     * @param nodeOrder The order of the node for which its left child's
     *                  order to be calculated
     * @return the order of the left child node
     */
    public static int getLeftChildOrder(int nodeOrder) {
        return (nodeOrder << 1) + 1;
    }

    /**
     * Returns the breadth-first order of the right child node of the node
     * with {@code nodeOrder}
     *
     * @param nodeOrder The order of the node for which its right child's
     *                  order to be calculated
     * @return the order of the right child node
     */
    public static int getRightChildOrder(int nodeOrder) {
        return (nodeOrder << 1) + 2;
    }

    /**
     * Returns the number of nodes in a tree with {@code depth} levels.
     *
     * @param depth The depth of the tree
     * @return the nodes in the tree
     */
    static int getNumberOfNodes(int depth) {
        return (1 << depth) - 1;
    }

    /**
     * Returns true if the node with {@code nodeOrder} is a leaf in a tree
     * with {@code depth} levels
     *
     * @param nodeOrder The order of the node to test
     * @param depth     The depth of the tree
     * @return true if the node is leaf, false otherwise
     */
    static boolean isLeaf(int nodeOrder, int depth) {
        checkTrue(depth > 0, "Invalid depth: " + depth);
        int leafLevel = depth - 1;
        int numberOfNodes = getNumberOfNodes(depth);
        int maxNodeOrder = numberOfNodes - 1;
        checkTrue(nodeOrder >= 0 && nodeOrder <= maxNodeOrder, "Invalid nodeOrder: " + nodeOrder + " in a tree with depth "
                + depth);
        int leftMostLeafOrder = MerkleTreeUtil.getLeftMostNodeOrderOnLevel(leafLevel);
        return nodeOrder >= leftMostLeafOrder;
    }

    /**
     * Returns the breadth-first order of the leftmost leaf in the
     * subtree selected by {@code nodeOrder} as the root of the subtree
     *
     * @param nodeOrder The order of the node as the root of the subtree
     * @param depth     The depth of the tree
     * @return the breadth-first order of the leftmost leaf under the
     * provided node
     */
    static int getLeftMostLeafUnderNode(int nodeOrder, int depth) {
        if (isLeaf(nodeOrder, depth)) {
            return nodeOrder;
        }

        int leafLevel = depth - 1;
        int levelOfNode = getLevelOfNode(nodeOrder);
        int distanceFromLeafLevel = depth - levelOfNode - 1;
        int leftMostNodeOrderOnLevel = getLeftMostNodeOrderOnLevel(levelOfNode);
        int relativeLevelOrder = nodeOrder - leftMostNodeOrderOnLevel;
        int leftMostLeaf = getLeftMostNodeOrderOnLevel(leafLevel);

        return leftMostLeaf + (2 << distanceFromLeafLevel - 1) * relativeLevelOrder;
    }

    /**
     * Returns the breadth-first order of the rightmost leaf in the
     * subtree selected by {@code nodeOrder} as the root of the subtree
     *
     * @param nodeOrder The order of the node as the root of the subtree
     * @param depth     The depth of the tree
     * @return the breadth-first order of the rightmost leaf under the
     * provided node
     */
    static int getRightMostLeafUnderNode(int nodeOrder, int depth) {
        if (isLeaf(nodeOrder, depth)) {
            return nodeOrder;
        }

        int levelOfNode = getLevelOfNode(nodeOrder);
        int distanceFromLeafLevel = depth - levelOfNode - 1;
        int leftMostLeafUnderNode = getLeftMostLeafUnderNode(nodeOrder, depth);
        int leavesOfSubtreeUnderNode = getNodesOnLevel(distanceFromLeafLevel);

        return leftMostLeafUnderNode + leavesOfSubtreeUnderNode - 1;
    }

    /**
     * Compares the provided local and the remote Merkle trees and
     * returns the breadth-first orders of the leaves that found to be
     * different.
     * <p>
     * This method compares only the leaves instead of traversing only
     * the subtrees that are different. This way possible hash collisions
     * inside the tree don't remain hidden and then syncing the clusters
     * can be initiated. This comes at the cost of comparing every leaf
     * at every comparison.
     *
     * @param local  The view of the local Merkle tree
     * @param remote The view of the remote Merkle tree
     * @return the order of the leaves found to be different
     */
    public static Collection<Integer> compareTrees(MerkleTreeView local, MerkleTreeView remote) {
        Collection<Integer> deltaOrders = new LinkedList<Integer>();
        MerkleTreeView baseTree = local.depth() <= remote.depth() ? local : remote;
        MerkleTreeView otherTree = local.depth() <= remote.depth() ? remote : local;
        int leafLevel = baseTree.depth() - 1;
        int numberOfLeaves = getNodesOnLevel(leafLevel);
        int leftMostLeaf = getLeftMostNodeOrderOnLevel(leafLevel);

        for (int leafOrder = leftMostLeaf; leafOrder < leftMostLeaf + numberOfLeaves; leafOrder++) {
            if (baseTree.getNodeHash(leafOrder) != otherTree.getNodeHash(leafOrder)) {
                deltaOrders.add(leafOrder);
            }
        }

        return deltaOrders;
    }

    /**
     * Writes the hashes of the leaves of a Merkle tree into the
     * provided {@link DataOutput}
     *
     * @param out            The data output to write the leaves into
     * @param merkleTreeView The Merkle tree which leaves to be written
     *                       into the data output
     * @throws IOException if an I/O error occurs
     */
    public static void writeLeaves(DataOutput out, MerkleTreeView merkleTreeView) throws IOException {
        int leafLevel = merkleTreeView.depth() - 1;
        int numberOfLeaves = getNodesOnLevel(leafLevel);
        int leftMostLeaf = getLeftMostNodeOrderOnLevel(leafLevel);

        out.writeInt(numberOfLeaves);

        for (int leafOrder = leftMostLeaf; leafOrder < leftMostLeaf + numberOfLeaves; leafOrder++) {
            out.writeInt(merkleTreeView.getNodeHash(leafOrder));
        }
    }

    /**
     * Creates a {@link RemoteMerkleTreeView} by reading the hashes of
     * the leaves of a Merkle tree from the provided {@link DataInput}
     *
     * @param in The data input the hashes to be read from
     * @return the view representing the remote Merkle tree
     * @throws IOException if an I/O error occurs
     */
    public static RemoteMerkleTreeView createRemoteMerkleTreeView(DataInput in) throws IOException {
        int numberOfLeaves = in.readInt();
        int depth = QuickMath.log2(numberOfLeaves << 1);
        int[] leaves = new int[numberOfLeaves];
        for (int i = 0; i < numberOfLeaves; i++) {
            leaves[i] = in.readInt();
        }

        return new RemoteMerkleTreeView(leaves, depth);
    }

    /**
     * @param merkleTreeOrderValuePairs an array of {@code [nodeOrder, hashValue]} pairs
     * @return set of given Merkle tree node orders
     */
    @Nonnull
    public static IntHashSet setOfNodeOrders(int[] merkleTreeOrderValuePairs) {
        assert merkleTreeOrderValuePairs.length % 2 == 0;
        IntHashSet merkleTreeOrderValues = new IntHashSet(merkleTreeOrderValuePairs.length / 2, -1);
        for (int i = 0; i < merkleTreeOrderValuePairs.length; i = i + 2) {
            merkleTreeOrderValues.add(merkleTreeOrderValuePairs[i]);
        }
        return merkleTreeOrderValues;
    }
}
