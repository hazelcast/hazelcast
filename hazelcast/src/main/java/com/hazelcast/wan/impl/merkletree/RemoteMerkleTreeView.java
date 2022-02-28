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

import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * Represents the readonly view of a remote Merkle Tree. Used to compare
 * remote and local Merkle trees.
 */
public class RemoteMerkleTreeView extends AbstractMerkleTreeView {

    /**
     * Creates the view of a remote Merkle tree with the provided {@code depth}
     * by reading the leaf hashes from the provided int array
     *
     * @param remoteTreeLeaves The int array that the remote tree's leaves
     *                         can be read from
     * @param depth            The depth of the remote tree
     */
    RemoteMerkleTreeView(int[] remoteTreeLeaves, int depth) {
        super(depth);

        int leafLevel = depth - 1;
        int numberOfLeaves = MerkleTreeUtil.getNodesOnLevel(leafLevel);
        int leftMostLeafOrder = MerkleTreeUtil.getLeftMostNodeOrderOnLevel(leafLevel);
        checkTrue(remoteTreeLeaves.length >= numberOfLeaves, "The provided array can't hold a tree with depth " + depth
                + ". Size of the provided array should be at least " + numberOfLeaves + ". Size of the provided array: "
                + remoteTreeLeaves.length);

        System.arraycopy(remoteTreeLeaves, 0, tree, leftMostLeafOrder, numberOfLeaves);

        buildTree();
    }

    private void buildTree() {
        for (int nodeOrder = leafLevelOrder - 1; nodeOrder >= 0; nodeOrder--) {
            int leftChildOrder = MerkleTreeUtil.getLeftChildOrder(nodeOrder);
            int rightChildOrder = MerkleTreeUtil.getRightChildOrder(nodeOrder);

            int leftChildHash = getNodeHash(leftChildOrder);
            int rightChildHash = getNodeHash(rightChildOrder);

            int newNodeHash = MerkleTreeUtil.sumHash(leftChildHash, rightChildHash);
            setNodeHash(nodeOrder, newNodeHash);
        }
    }

    @Override
    public int getNodeHash(int nodeOrder) {
        return tree[nodeOrder];
    }
}
