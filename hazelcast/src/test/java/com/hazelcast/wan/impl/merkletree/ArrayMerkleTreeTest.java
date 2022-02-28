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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ArrayMerkleTreeTest {

    @Test(expected = IllegalArgumentException.class)
    public void testDepthBelowMinDepthThrows() {
        new ArrayMerkleTree(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDepthAboveMaxDepthThrows() {
        new ArrayMerkleTree(28);
    }

    @Test
    public void testDepth() {
        MerkleTree merkleTree = new ArrayMerkleTree(3);

        assertEquals(3, merkleTree.depth());
    }

    @Test
    public void testFootprint() {
        MerkleTree merkleTree1 = new ArrayMerkleTree(3);
        MerkleTree merkleTree2 = new ArrayMerkleTree(3);

        for (int i = 0; i < 10; i++) {
            merkleTree1.updateAdd(i, i);
        }

        for (int i = 0; i < 100; i++) {
            merkleTree2.updateAdd(i, i);
        }

        assertEquals(merkleTree2.footprint(), merkleTree1.footprint());
    }

    @Test
    public void testFootprintDoesNotChange() {
        MerkleTree merkleTree = new ArrayMerkleTree(3);

        long footprintBeforeAdd = merkleTree.footprint();
        for (int i = 0; i < 100; i++) {
            merkleTree.updateAdd(i, i);
        }
        long footprintAfterAdd = merkleTree.footprint();

        assertEquals(footprintAfterAdd, footprintBeforeAdd);
    }

    @Test
    public void testUpdateAdd() {
        MerkleTree merkleTree = new ArrayMerkleTree(3);

        merkleTree.updateAdd(1, 1);
        merkleTree.updateAdd(2, 2);
        merkleTree.updateAdd(3, 3);

        int expectedHash = 0;
        expectedHash = MerkleTreeUtil.addHash(expectedHash, 1);
        expectedHash = MerkleTreeUtil.addHash(expectedHash, 2);
        expectedHash = MerkleTreeUtil.addHash(expectedHash, 3);

        int nodeHash = merkleTree.getNodeHash(5);

        assertEquals(expectedHash, nodeHash);
    }

    @Test
    public void testUpdateAddUpdateBranch() {
        MerkleTree merkleTree = new ArrayMerkleTree(3);

        merkleTree.updateAdd(-3, 4);
        merkleTree.updateAdd(-2, 2);
        merkleTree.updateAdd(-1, 1);
        merkleTree.updateAdd(0, 0);
        merkleTree.updateAdd(1, -1);
        merkleTree.updateAdd(2, -2);
        merkleTree.updateAdd(3, -3);

        int expectedHashNode5 = 0;
        expectedHashNode5 = MerkleTreeUtil.addHash(expectedHashNode5, -1);
        expectedHashNode5 = MerkleTreeUtil.addHash(expectedHashNode5, -2);
        expectedHashNode5 = MerkleTreeUtil.addHash(expectedHashNode5, -3);

        int expectedHashNode4 = 0;
        expectedHashNode4 = MerkleTreeUtil.addHash(expectedHashNode4, 1);
        expectedHashNode4 = MerkleTreeUtil.addHash(expectedHashNode4, 2);
        expectedHashNode4 = MerkleTreeUtil.addHash(expectedHashNode4, 4);

        int expectedHashNode1 = MerkleTreeUtil.sumHash(0, expectedHashNode4);
        int expectedHashNode2 = MerkleTreeUtil.sumHash(0, expectedHashNode5);
        int expectedHashNode0 = MerkleTreeUtil.sumHash(expectedHashNode1, expectedHashNode2);

        assertEquals(expectedHashNode0, merkleTree.getNodeHash(0));
        assertEquals(expectedHashNode1, merkleTree.getNodeHash(1));
        assertEquals(expectedHashNode2, merkleTree.getNodeHash(2));
        assertEquals(0, merkleTree.getNodeHash(3));
        assertEquals(expectedHashNode4, merkleTree.getNodeHash(4));
        assertEquals(expectedHashNode5, merkleTree.getNodeHash(5));
        assertEquals(0, merkleTree.getNodeHash(6));
    }

    @Test
    public void testUpdateReplaceUpdateBranch() {
        MerkleTree merkleTree = new ArrayMerkleTree(3);

        merkleTree.updateAdd(-3, 4);
        merkleTree.updateAdd(-2, 2);
        merkleTree.updateAdd(-1, 1);
        merkleTree.updateAdd(0, 0);
        merkleTree.updateAdd(1, -1);
        merkleTree.updateAdd(2, -2);
        merkleTree.updateAdd(3, -3);

        merkleTree.updateReplace(3, -3, -5);

        int expectedHashNode5 = 0;
        expectedHashNode5 = MerkleTreeUtil.addHash(expectedHashNode5, -1);
        expectedHashNode5 = MerkleTreeUtil.addHash(expectedHashNode5, -2);
        expectedHashNode5 = MerkleTreeUtil.addHash(expectedHashNode5, -5);

        int expectedHashNode4 = 0;
        expectedHashNode4 = MerkleTreeUtil.addHash(expectedHashNode4, 1);
        expectedHashNode4 = MerkleTreeUtil.addHash(expectedHashNode4, 2);
        expectedHashNode4 = MerkleTreeUtil.addHash(expectedHashNode4, 4);

        int expectedHashNode1 = MerkleTreeUtil.sumHash(0, expectedHashNode4);
        int expectedHashNode2 = MerkleTreeUtil.sumHash(0, expectedHashNode5);
        int expectedHashNode0 = MerkleTreeUtil.sumHash(expectedHashNode1, expectedHashNode2);

        assertEquals(expectedHashNode0, merkleTree.getNodeHash(0));
        assertEquals(expectedHashNode1, merkleTree.getNodeHash(1));
        assertEquals(expectedHashNode2, merkleTree.getNodeHash(2));
        assertEquals(0, merkleTree.getNodeHash(3));
        assertEquals(expectedHashNode4, merkleTree.getNodeHash(4));
        assertEquals(expectedHashNode5, merkleTree.getNodeHash(5));
        assertEquals(0, merkleTree.getNodeHash(6));
    }

    @Test
    public void testUpdateRemoveUpdateBranch() {
        MerkleTree merkleTree = new ArrayMerkleTree(3);

        merkleTree.updateAdd(-3, 4);
        merkleTree.updateAdd(-2, 2);
        merkleTree.updateAdd(-1, 1);
        merkleTree.updateAdd(0, 0);
        merkleTree.updateAdd(1, -1);
        merkleTree.updateAdd(2, -2);
        merkleTree.updateAdd(3, -3);

        merkleTree.updateRemove(3, -3);

        int expectedHashNode5 = 0;
        expectedHashNode5 = MerkleTreeUtil.addHash(expectedHashNode5, -1);
        expectedHashNode5 = MerkleTreeUtil.addHash(expectedHashNode5, -2);

        int expectedHashNode4 = 0;
        expectedHashNode4 = MerkleTreeUtil.addHash(expectedHashNode4, 1);
        expectedHashNode4 = MerkleTreeUtil.addHash(expectedHashNode4, 2);
        expectedHashNode4 = MerkleTreeUtil.addHash(expectedHashNode4, 4);

        int expectedHashNode1 = MerkleTreeUtil.sumHash(0, expectedHashNode4);
        int expectedHashNode2 = MerkleTreeUtil.sumHash(0, expectedHashNode5);
        int expectedHashNode0 = MerkleTreeUtil.sumHash(expectedHashNode1, expectedHashNode2);

        assertEquals(expectedHashNode0, merkleTree.getNodeHash(0));
        assertEquals(expectedHashNode1, merkleTree.getNodeHash(1));
        assertEquals(expectedHashNode2, merkleTree.getNodeHash(2));
        assertEquals(0, merkleTree.getNodeHash(3));
        assertEquals(expectedHashNode4, merkleTree.getNodeHash(4));
        assertEquals(expectedHashNode5, merkleTree.getNodeHash(5));
        assertEquals(0, merkleTree.getNodeHash(6));
    }

    @Test
    public void testUpdateReplace() {
        MerkleTree merkleTree = new ArrayMerkleTree(3);

        merkleTree.updateAdd(1, 1);
        merkleTree.updateAdd(2, 2);
        merkleTree.updateAdd(3, 3);
        merkleTree.updateReplace(2, 2, 4);

        int expectedHash = 0;
        expectedHash = MerkleTreeUtil.addHash(expectedHash, 1);
        expectedHash = MerkleTreeUtil.addHash(expectedHash, 3);
        expectedHash = MerkleTreeUtil.addHash(expectedHash, 4);

        int nodeHash = merkleTree.getNodeHash(5);

        assertEquals(expectedHash, nodeHash);
    }

    @Test
    public void testUpdateRemove() {
        MerkleTree merkleTree = new ArrayMerkleTree(3);

        merkleTree.updateAdd(1, 1);
        merkleTree.updateAdd(2, 2);
        merkleTree.updateAdd(3, 3);
        merkleTree.updateRemove(2, 2);

        int expectedHash = 0;
        expectedHash = MerkleTreeUtil.addHash(expectedHash, 1);
        expectedHash = MerkleTreeUtil.addHash(expectedHash, 3);

        int nodeHash = merkleTree.getNodeHash(5);

        assertEquals(expectedHash, nodeHash);
    }

    @Test
    public void testTreeDepthsDontImpactNodeHashes() {
        MerkleTree merkleTreeShallow = new ArrayMerkleTree(2);
        MerkleTree merkleTreeDeep = new ArrayMerkleTree(4);

        merkleTreeShallow.updateAdd(0x80000000, 1); // leaf 1
        merkleTreeShallow.updateAdd(0xA0000000, 2); // leaf 1
        merkleTreeShallow.updateAdd(0xC0000000, 3); // leaf 1
        merkleTreeShallow.updateAdd(0xE0000000, 4); // leaf 1
        merkleTreeShallow.updateAdd(0x00000000, 5); // leaf 2
        merkleTreeShallow.updateAdd(0x20000000, 6); // leaf 2
        merkleTreeShallow.updateAdd(0x40000000, 7); // leaf 2
        merkleTreeShallow.updateAdd(0x60000000, 8); // leaf 2

        merkleTreeDeep.updateAdd(0x80000000, 1); // leaf 7
        merkleTreeDeep.updateAdd(0xA0000000, 2); // leaf 8
        merkleTreeDeep.updateAdd(0xC0000000, 3); // leaf 9
        merkleTreeDeep.updateAdd(0xE0000000, 4); // leaf 10
        merkleTreeDeep.updateAdd(0x00000000, 5); // leaf 11
        merkleTreeDeep.updateAdd(0x20000000, 6); // leaf 12
        merkleTreeDeep.updateAdd(0x40000000, 7); // leaf 13
        merkleTreeDeep.updateAdd(0x60000000, 8); // leaf 14

        verifyTreesAreSameOnCommonLevels(merkleTreeShallow, merkleTreeDeep);

        merkleTreeShallow.updateReplace(0xE0000000, 4, 42);
        merkleTreeDeep.updateReplace(0xE0000000, 4, 42);

        verifyTreesAreSameOnCommonLevels(merkleTreeShallow, merkleTreeDeep);

        merkleTreeShallow.updateRemove(0xE0000000, 42);
        merkleTreeDeep.updateRemove(0xE0000000, 42);

        verifyTreesAreSameOnCommonLevels(merkleTreeShallow, merkleTreeDeep);
    }

    @Test
    public void testClear() {
        MerkleTree merkleTree = new ArrayMerkleTree(4);

        merkleTree.updateAdd(0x80000000, 1); // leaf 7
        merkleTree.updateAdd(0xA0000000, 2); // leaf 8
        merkleTree.updateAdd(0xC0000000, 3); // leaf 9
        merkleTree.updateAdd(0xE0000000, 4); // leaf 10
        merkleTree.updateAdd(0x00000000, 5); // leaf 11
        merkleTree.updateAdd(0x20000000, 6); // leaf 12
        merkleTree.updateAdd(0x40000000, 7); // leaf 13
        merkleTree.updateAdd(0x60000000, 8); // leaf 14

        assertNotEquals(0, merkleTree.getNodeHash(0));

        merkleTree.clear();

        for (int nodeOrder = 0; nodeOrder < MerkleTreeUtil.getNumberOfNodes(merkleTree.depth()); nodeOrder++) {
            assertEquals(0, merkleTree.getNodeHash(nodeOrder));
        }
    }

    private void verifyTreesAreSameOnCommonLevels(MerkleTree merkleTreeShallow, MerkleTree merkleTreeDeep) {
        for (int i = 0; i < MerkleTreeUtil.getNumberOfNodes(merkleTreeShallow.depth()); i++) {
            assertEquals(merkleTreeShallow.getNodeHash(i), merkleTreeDeep.getNodeHash(i));
        }
    }
}
