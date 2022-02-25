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

/**
 * Base class for the {@link MerkleTreeView} implementations
 */
abstract class AbstractMerkleTreeView implements MerkleTreeView {
    private static final int MIN_DEPTH = 2;
    private static final int MAX_DEPTH = 27;

    protected final int[] tree;
    protected final int depth;
    protected final int leafLevelOrder;

    AbstractMerkleTreeView(int depth) {
        if (depth < MIN_DEPTH || depth > MAX_DEPTH) {
            throw new IllegalArgumentException("Parameter depth " + depth + " is outside of the allowed range "
                    + MIN_DEPTH + "-" + MAX_DEPTH + ". ");
        }

        this.leafLevelOrder = MerkleTreeUtil.getLeftMostNodeOrderOnLevel(depth - 1);
        this.depth = depth;

        final int nodes = MerkleTreeUtil.getNumberOfNodes(depth);
        this.tree = new int[nodes];
    }

    protected void setNodeHash(int nodeOrder, int hash) {
        tree[nodeOrder] = hash;
    }

    @Override
    public int depth() {
        return depth;
    }
}
