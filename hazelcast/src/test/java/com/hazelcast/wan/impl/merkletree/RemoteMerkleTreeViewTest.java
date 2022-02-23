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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RemoteMerkleTreeViewTest {

    @Test
    public void testConstructorProperArray() {
        int[] tree = new int[4];
        new RemoteMerkleTreeView(tree, 3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorShortArrayThrows() {
        int[] tree = new int[3];
        new RemoteMerkleTreeView(tree, 3);
    }

    @Test
    public void testDepthWithArrayConstructor() {
        int[] tree = new int[7];
        RemoteMerkleTreeView merkleTreeView = new RemoteMerkleTreeView(tree, 3);

        assertEquals(3, merkleTreeView.depth());
    }

    @Test
    public void testHashesWithByteArrayConstructor() {
        int[] tree = new int[4];
        for (int i = 0; i < 4; i++) {
            tree[i] = 4 - i;
        }

        RemoteMerkleTreeView merkleTreeView = new RemoteMerkleTreeView(tree, 3);

        verifyMerkleTreeView(merkleTreeView);
    }

    private void verifyMerkleTreeView(RemoteMerkleTreeView merkleTreeView) {
        for (int i = 3; i < 7; i++) {
            assertEquals(7 - i, merkleTreeView.getNodeHash(i));
        }
        int expectedHashNode1 = MerkleTreeUtil.sumHash(merkleTreeView.getNodeHash(3), merkleTreeView.getNodeHash(4));
        int expectedHashNode2 = MerkleTreeUtil.sumHash(merkleTreeView.getNodeHash(5), merkleTreeView.getNodeHash(6));
        int expectedHashNode0 = MerkleTreeUtil.sumHash(expectedHashNode1, expectedHashNode2);
        assertEquals(expectedHashNode1, merkleTreeView.getNodeHash(1));
        assertEquals(expectedHashNode2, merkleTreeView.getNodeHash(2));
        assertEquals(expectedHashNode0, merkleTreeView.getNodeHash(0));
    }
}
