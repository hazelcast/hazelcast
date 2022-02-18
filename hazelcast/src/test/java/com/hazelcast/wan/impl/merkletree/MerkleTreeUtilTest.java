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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MerkleTreeUtilTest {

    @Test
    public void testGetLevelOfNode() {
        assertEquals(0, MerkleTreeUtil.getLevelOfNode(0));
        assertEquals(1, MerkleTreeUtil.getLevelOfNode(1));
        assertEquals(1, MerkleTreeUtil.getLevelOfNode(2));
        assertEquals(2, MerkleTreeUtil.getLevelOfNode(3));
        assertEquals(2, MerkleTreeUtil.getLevelOfNode(6));
        assertEquals(3, MerkleTreeUtil.getLevelOfNode(7));
        assertEquals(3, MerkleTreeUtil.getLevelOfNode(14));
        assertEquals(4, MerkleTreeUtil.getLevelOfNode(15));
        assertEquals(4, MerkleTreeUtil.getLevelOfNode(30));
    }

    @Test
    public void testGetNodesOnLevel() {
        assertEquals(1, MerkleTreeUtil.getNodesOnLevel(0));
        assertEquals(2, MerkleTreeUtil.getNodesOnLevel(1));
        assertEquals(4, MerkleTreeUtil.getNodesOnLevel(2));
        assertEquals(8, MerkleTreeUtil.getNodesOnLevel(3));
        assertEquals(16, MerkleTreeUtil.getNodesOnLevel(4));
    }

    @Test
    public void testGetLeftMostNodeOnLevel() {
        assertEquals(0, MerkleTreeUtil.getLeftMostNodeOrderOnLevel(0));
        assertEquals(1, MerkleTreeUtil.getLeftMostNodeOrderOnLevel(1));
        assertEquals(3, MerkleTreeUtil.getLeftMostNodeOrderOnLevel(2));
        assertEquals(7, MerkleTreeUtil.getLeftMostNodeOrderOnLevel(3));
        assertEquals(15, MerkleTreeUtil.getLeftMostNodeOrderOnLevel(4));
    }

    @Test
    public void testGetParentOrder() {
        assertEquals(0, MerkleTreeUtil.getParentOrder(1));
        assertEquals(0, MerkleTreeUtil.getParentOrder(2));
        assertEquals(1, MerkleTreeUtil.getParentOrder(3));
        assertEquals(1, MerkleTreeUtil.getParentOrder(4));
        assertEquals(2, MerkleTreeUtil.getParentOrder(5));
        assertEquals(2, MerkleTreeUtil.getParentOrder(6));
    }

    @Test
    public void testLeftChildOrder() {
        assertEquals(1, MerkleTreeUtil.getLeftChildOrder(0));
        assertEquals(3, MerkleTreeUtil.getLeftChildOrder(1));
        assertEquals(5, MerkleTreeUtil.getLeftChildOrder(2));
        assertEquals(7, MerkleTreeUtil.getLeftChildOrder(3));
        assertEquals(11, MerkleTreeUtil.getLeftChildOrder(5));
    }

    @Test
    public void testRightChildOrder() {
        assertEquals(2, MerkleTreeUtil.getRightChildOrder(0));
        assertEquals(4, MerkleTreeUtil.getRightChildOrder(1));
        assertEquals(6, MerkleTreeUtil.getRightChildOrder(2));
        assertEquals(8, MerkleTreeUtil.getRightChildOrder(3));
        assertEquals(12, MerkleTreeUtil.getRightChildOrder(5));
    }

    @Test
    public void testGetHashStepForLevel() {
        assertEquals(1L << 32, MerkleTreeUtil.getNodeHashRangeOnLevel(0));
        assertEquals(1L << 31, MerkleTreeUtil.getNodeHashRangeOnLevel(1));
        assertEquals(1L << 30, MerkleTreeUtil.getNodeHashRangeOnLevel(2));
        assertEquals(1L << 29, MerkleTreeUtil.getNodeHashRangeOnLevel(3));
        assertEquals(1L << 28, MerkleTreeUtil.getNodeHashRangeOnLevel(4));
    }

    @Test
    public void testGetLeafOrderForHash() {
        // Integer.MIN_VALUE
        assertEquals(0, MerkleTreeUtil.getLeafOrderForHash(0x80000000, 0));
        assertEquals(1, MerkleTreeUtil.getLeafOrderForHash(0x80000000, 1));
        assertEquals(3, MerkleTreeUtil.getLeafOrderForHash(0x80000000, 2));
        assertEquals(7, MerkleTreeUtil.getLeafOrderForHash(0x80000000, 3));

        // Integer.MAX_VALUE
        assertEquals(0, MerkleTreeUtil.getLeafOrderForHash(0x7FFFFFFF, 0));
        assertEquals(2, MerkleTreeUtil.getLeafOrderForHash(0x7FFFFFFF, 1));
        assertEquals(6, MerkleTreeUtil.getLeafOrderForHash(0x7FFFFFFF, 2));
        assertEquals(14, MerkleTreeUtil.getLeafOrderForHash(0x7FFFFFFF, 3));

        // 0
        assertEquals(0, MerkleTreeUtil.getLeafOrderForHash(0x000000000, 0));
        assertEquals(2, MerkleTreeUtil.getLeafOrderForHash(0x000000000, 1));
        assertEquals(5, MerkleTreeUtil.getLeafOrderForHash(0x000000000, 2));
        assertEquals(11, MerkleTreeUtil.getLeafOrderForHash(0x000000000, 3));

        // 1
        assertEquals(0, MerkleTreeUtil.getLeafOrderForHash(0x000000001, 0));
        assertEquals(2, MerkleTreeUtil.getLeafOrderForHash(0x000000001, 1));
        assertEquals(5, MerkleTreeUtil.getLeafOrderForHash(0x000000001, 2));
        assertEquals(11, MerkleTreeUtil.getLeafOrderForHash(0x000000001, 3));

        // -1
        assertEquals(0, MerkleTreeUtil.getLeafOrderForHash(0xFFFFFFFF, 0));
        assertEquals(1, MerkleTreeUtil.getLeafOrderForHash(0xFFFFFFFF, 1));
        assertEquals(4, MerkleTreeUtil.getLeafOrderForHash(0xFFFFFFFF, 2));
        assertEquals(10, MerkleTreeUtil.getLeafOrderForHash(0xFFFFFFFF, 3));

        // Integer.MIN_VALUE / 4 * 2 - 1
        assertEquals(0, MerkleTreeUtil.getLeafOrderForHash(0xBFFFFFFF, 0));
        assertEquals(1, MerkleTreeUtil.getLeafOrderForHash(0xBFFFFFFF, 1));
        assertEquals(3, MerkleTreeUtil.getLeafOrderForHash(0xBFFFFFFF, 2));
        assertEquals(8, MerkleTreeUtil.getLeafOrderForHash(0xBFFFFFFF, 3));

        // Integer.MIN_VALUE / 4 * 2
        assertEquals(0, MerkleTreeUtil.getLeafOrderForHash(0xC0000000, 0));
        assertEquals(1, MerkleTreeUtil.getLeafOrderForHash(0xC0000000, 1));
        assertEquals(4, MerkleTreeUtil.getLeafOrderForHash(0xC0000000, 2));
        assertEquals(9, MerkleTreeUtil.getLeafOrderForHash(0xC0000000, 3));

        // Integer.MAX_VALUE / 4 * 2 + 1
        assertEquals(0, MerkleTreeUtil.getLeafOrderForHash(0x3FFFFFFF, 0));
        assertEquals(2, MerkleTreeUtil.getLeafOrderForHash(0x3FFFFFFF, 1));
        assertEquals(5, MerkleTreeUtil.getLeafOrderForHash(0x3FFFFFFF, 2));
        assertEquals(12, MerkleTreeUtil.getLeafOrderForHash(0x3FFFFFFF, 3));

        // Integer.MAX_VALUE / 4 * 2 + 2
        assertEquals(0, MerkleTreeUtil.getLeafOrderForHash(0x40000000, 0));
        assertEquals(2, MerkleTreeUtil.getLeafOrderForHash(0x40000000, 1));
        assertEquals(6, MerkleTreeUtil.getLeafOrderForHash(0x40000000, 2));
        assertEquals(13, MerkleTreeUtil.getLeafOrderForHash(0x40000000, 3));
    }

    @Test
    public void testGetNodeRanges() {
        // level 0
        assertEquals(0x80000000, MerkleTreeUtil.getNodeRangeLow(0));
        assertEquals(0x7FFFFFFF, MerkleTreeUtil.getNodeRangeHigh(0));

        // level 1
        assertEquals(0x80000000, MerkleTreeUtil.getNodeRangeLow(1));
        assertEquals(0xFFFFFFFF, MerkleTreeUtil.getNodeRangeHigh(1));
        assertEquals(0x00000000, MerkleTreeUtil.getNodeRangeLow(2));
        assertEquals(0x7FFFFFFF, MerkleTreeUtil.getNodeRangeHigh(2));

        // level 2
        assertEquals(0x80000000, MerkleTreeUtil.getNodeRangeLow(3));
        assertEquals(0xBFFFFFFF, MerkleTreeUtil.getNodeRangeHigh(3));
        assertEquals(0xC0000000, MerkleTreeUtil.getNodeRangeLow(4));
        assertEquals(0xFFFFFFFF, MerkleTreeUtil.getNodeRangeHigh(4));
        assertEquals(0x00000000, MerkleTreeUtil.getNodeRangeLow(5));
        assertEquals(0x3FFFFFFF, MerkleTreeUtil.getNodeRangeHigh(5));
        assertEquals(0x40000000, MerkleTreeUtil.getNodeRangeLow(6));
        assertEquals(0x7FFFFFFF, MerkleTreeUtil.getNodeRangeHigh(6));

        // level 3
        assertEquals(0x80000000, MerkleTreeUtil.getNodeRangeLow(7));
        assertEquals(0x9FFFFFFF, MerkleTreeUtil.getNodeRangeHigh(7));
        assertEquals(0xA0000000, MerkleTreeUtil.getNodeRangeLow(8));
        assertEquals(0xBFFFFFFF, MerkleTreeUtil.getNodeRangeHigh(8));
        assertEquals(0xC0000000, MerkleTreeUtil.getNodeRangeLow(9));
        assertEquals(0xDFFFFFFF, MerkleTreeUtil.getNodeRangeHigh(9));
        assertEquals(0xE0000000, MerkleTreeUtil.getNodeRangeLow(10));
        assertEquals(0xFFFFFFFF, MerkleTreeUtil.getNodeRangeHigh(10));
        assertEquals(0x00000000, MerkleTreeUtil.getNodeRangeLow(11));
        assertEquals(0x1FFFFFFF, MerkleTreeUtil.getNodeRangeHigh(11));
        assertEquals(0x20000000, MerkleTreeUtil.getNodeRangeLow(12));
        assertEquals(0x3FFFFFFF, MerkleTreeUtil.getNodeRangeHigh(12));
        assertEquals(0x40000000, MerkleTreeUtil.getNodeRangeLow(13));
        assertEquals(0x5FFFFFFF, MerkleTreeUtil.getNodeRangeHigh(13));
        assertEquals(0x60000000, MerkleTreeUtil.getNodeRangeLow(14));
        assertEquals(0x7FFFFFFF, MerkleTreeUtil.getNodeRangeHigh(14));
    }

    @Test
    public void testAddHashIsAssociative() {
        int hash = MerkleTreeUtil.addHash(0, 1);
        hash = MerkleTreeUtil.addHash(hash, 2);
        hash = MerkleTreeUtil.addHash(hash, 3);

        int hash2 = MerkleTreeUtil.addHash(0, 3);
        hash2 = MerkleTreeUtil.addHash(hash2, 1);
        hash2 = MerkleTreeUtil.addHash(hash2, 2);

        assertEquals(hash2, hash);
    }

    @Test
    public void testRemoveHashIsAssociative() {
        int hash = MerkleTreeUtil.addHash(0, 1);
        hash = MerkleTreeUtil.addHash(hash, 2);
        hash = MerkleTreeUtil.addHash(hash, 3);
        hash = MerkleTreeUtil.addHash(hash, 4);
        hash = MerkleTreeUtil.addHash(hash, 5);

        int hash2 = hash;

        hash = MerkleTreeUtil.removeHash(hash, 5);
        hash = MerkleTreeUtil.removeHash(hash, 4);
        hash = MerkleTreeUtil.removeHash(hash, 3);
        hash = MerkleTreeUtil.removeHash(hash, 2);

        hash2 = MerkleTreeUtil.removeHash(hash2, 2);
        hash2 = MerkleTreeUtil.removeHash(hash2, 5);
        hash2 = MerkleTreeUtil.removeHash(hash2, 3);
        hash2 = MerkleTreeUtil.removeHash(hash2, 4);

        assertEquals(hash2, hash);
    }

    @Test
    public void testRemovingHashAndNotAddingHashResultsTheSame() {
        int hash = MerkleTreeUtil.addHash(0, 1);
        hash = MerkleTreeUtil.addHash(hash, 2);
        hash = MerkleTreeUtil.addHash(hash, 3);
        hash = MerkleTreeUtil.addHash(hash, 4);
        hash = MerkleTreeUtil.addHash(hash, 5);

        int hash2 = MerkleTreeUtil.addHash(0, 1);
        hash2 = MerkleTreeUtil.addHash(hash2, 2);
        // not adding 3 here
        hash2 = MerkleTreeUtil.addHash(hash2, 4);
        hash2 = MerkleTreeUtil.addHash(hash2, 5);

        // removing hash of 3
        hash = MerkleTreeUtil.removeHash(hash, 3);

        assertEquals(hash2, hash);
    }

    @Test
    public void testIsLeaf() {
        assertTrue(MerkleTreeUtil.isLeaf(0, 1));
        assertFalse(MerkleTreeUtil.isLeaf(0, 2));
        assertTrue(MerkleTreeUtil.isLeaf(1, 2));
        assertTrue(MerkleTreeUtil.isLeaf(2, 2));

        assertFalse(MerkleTreeUtil.isLeaf(1, 3));
        assertFalse(MerkleTreeUtil.isLeaf(2, 3));
        assertTrue(MerkleTreeUtil.isLeaf(3, 3));
        assertTrue(MerkleTreeUtil.isLeaf(6, 3));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsLeafThrowsOnInvalidDepth() {
        MerkleTreeUtil.isLeaf(0, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsLeafThrowsOnNegativeNodeOrder() {
        MerkleTreeUtil.isLeaf(-1, 3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsLeafThrowsOnNodeOrderBeyondMaxNodeOrder() {
        MerkleTreeUtil.isLeaf(7, 3);
    }

    @Test
    public void testGetLeftMostLeafUnderNode() {
        // depth 1
        assertEquals(0, MerkleTreeUtil.getLeftMostLeafUnderNode(0, 1));

        // depth 2
        assertEquals(1, MerkleTreeUtil.getLeftMostLeafUnderNode(0, 2));
        assertEquals(1, MerkleTreeUtil.getLeftMostLeafUnderNode(1, 2));
        assertEquals(2, MerkleTreeUtil.getLeftMostLeafUnderNode(2, 2));

        // depth 3
        assertEquals(3, MerkleTreeUtil.getLeftMostLeafUnderNode(0, 3));
        assertEquals(3, MerkleTreeUtil.getLeftMostLeafUnderNode(1, 3));
        assertEquals(5, MerkleTreeUtil.getLeftMostLeafUnderNode(2, 3));

        // depth 4
        assertEquals(7, MerkleTreeUtil.getLeftMostLeafUnderNode(0, 4));
        assertEquals(7, MerkleTreeUtil.getLeftMostLeafUnderNode(1, 4));
        assertEquals(11, MerkleTreeUtil.getLeftMostLeafUnderNode(2, 4));
        assertEquals(7, MerkleTreeUtil.getLeftMostLeafUnderNode(3, 4));
        assertEquals(9, MerkleTreeUtil.getLeftMostLeafUnderNode(4, 4));
        assertEquals(11, MerkleTreeUtil.getLeftMostLeafUnderNode(5, 4));
        assertEquals(13, MerkleTreeUtil.getLeftMostLeafUnderNode(6, 4));
    }

    @Test
    public void testGetRightMostLeafUnderNode() {
        // depth 1
        assertEquals(0, MerkleTreeUtil.getRightMostLeafUnderNode(0, 1));

        // depth 2
        assertEquals(2, MerkleTreeUtil.getRightMostLeafUnderNode(0, 2));
        assertEquals(1, MerkleTreeUtil.getRightMostLeafUnderNode(1, 2));
        assertEquals(2, MerkleTreeUtil.getRightMostLeafUnderNode(2, 2));

        // depth 3
        assertEquals(6, MerkleTreeUtil.getRightMostLeafUnderNode(0, 3));
        assertEquals(4, MerkleTreeUtil.getRightMostLeafUnderNode(1, 3));
        assertEquals(6, MerkleTreeUtil.getRightMostLeafUnderNode(2, 3));

        // depth 4
        assertEquals(14, MerkleTreeUtil.getRightMostLeafUnderNode(0, 4));
        assertEquals(10, MerkleTreeUtil.getRightMostLeafUnderNode(1, 4));
        assertEquals(14, MerkleTreeUtil.getRightMostLeafUnderNode(2, 4));
        assertEquals(8, MerkleTreeUtil.getRightMostLeafUnderNode(3, 4));
        assertEquals(10, MerkleTreeUtil.getRightMostLeafUnderNode(4, 4));
        assertEquals(12, MerkleTreeUtil.getRightMostLeafUnderNode(5, 4));
        assertEquals(14, MerkleTreeUtil.getRightMostLeafUnderNode(6, 4));
    }

    @Test
    public void testCompareIdenticalTrees() {
        int[] tree = new int[4];
        for (int i = 0; i < 4; i++) {
            tree[i] = 4 - i;
        }
        MerkleTreeView localTree = new RemoteMerkleTreeView(tree, 3);
        MerkleTreeView remoteTree = new RemoteMerkleTreeView(tree, 3);

        Collection<Integer> deltaOrders = MerkleTreeUtil.compareTrees(localTree, remoteTree);
        assertTrue(deltaOrders.isEmpty());
    }

    @Test
    public void testCompareDifferentTreesLocalDeeper() {
        int numberOfLocalTreeLeaves = 15;
        int[] localTreeLeaves = new int[numberOfLocalTreeLeaves];
        for (int i = 0; i < numberOfLocalTreeLeaves; i++) {
            localTreeLeaves[i] = i;
        }

        int numberOfRemoteTreeNodes = 7;
        int[] remoteTreeLeaves = new int[numberOfRemoteTreeNodes];
        remoteTreeLeaves[0] = MerkleTreeUtil.sumHash(0, 1);
        remoteTreeLeaves[1] = MerkleTreeUtil.sumHash(2, 3);
        remoteTreeLeaves[2] = MerkleTreeUtil.sumHash(42, 5);
        remoteTreeLeaves[3] = MerkleTreeUtil.sumHash(6, 7);

        MerkleTreeView localTreeView = new RemoteMerkleTreeView(localTreeLeaves, 4);
        MerkleTreeView remoteTreeView = new RemoteMerkleTreeView(remoteTreeLeaves, 3);

        Collection<Integer> deltaOrders = MerkleTreeUtil.compareTrees(localTreeView, remoteTreeView);
        assertEquals(1, deltaOrders.size());
        assertTrue(deltaOrders.contains(5));
    }

    @Test
    public void testCompareDifferentTreesRemoteDeeper() {
        int numberOfLocalTreeLeaves = 4;
        int[] localTreeLeaves = new int[numberOfLocalTreeLeaves];
        localTreeLeaves[0] = MerkleTreeUtil.sumHash(0, 1);
        localTreeLeaves[1] = MerkleTreeUtil.sumHash(2, 3);
        localTreeLeaves[2] = MerkleTreeUtil.sumHash(42, 5);
        localTreeLeaves[3] = MerkleTreeUtil.sumHash(6, 7);

        int numberOfRemoteTreeNodes = 8;
        int[] remoteTreeLeaves = new int[numberOfRemoteTreeNodes];
        for (int i = 0; i < numberOfRemoteTreeNodes; i++) {
            remoteTreeLeaves[i] = i;
        }

        MerkleTreeView localTreeView = new RemoteMerkleTreeView(localTreeLeaves, 3);
        MerkleTreeView remoteTreeView = new RemoteMerkleTreeView(remoteTreeLeaves, 4);

        Collection<Integer> deltaOrders = MerkleTreeUtil.compareTrees(localTreeView, remoteTreeView);
        assertEquals(1, deltaOrders.size());
        assertTrue(deltaOrders.contains(5));
    }

    @Test
    public void testCompareTreesCatchesCollision() {
        int numberOfLocalTreeLeaves = 4;
        int[] localTreeLeaves = new int[numberOfLocalTreeLeaves];
        for (int i = 0; i < numberOfLocalTreeLeaves; i++) {
            localTreeLeaves[i] = i;
        }

        int numberOfRemoteTreeNodes = 4;
        int[] remoteTreeLeaves = new int[numberOfRemoteTreeNodes];
        for (int i = 0; i < numberOfRemoteTreeNodes; i++) {
            remoteTreeLeaves[i] = i;
        }

        // we cause a collision here that compareTrees() will notice
        // hash(node5,node6) will produce the same hash for node2 in both trees
        // localTreeLeaves: hash(2,3)=5
        // remoteTreeLeaves: hash(1,4)=5
        localTreeLeaves[2] = 1;
        localTreeLeaves[3] = 4;

        MerkleTreeView localTreeView = new RemoteMerkleTreeView(localTreeLeaves, 3);
        MerkleTreeView remoteTreeView = new RemoteMerkleTreeView(remoteTreeLeaves, 3);

        Collection<Integer> deltaOrders = MerkleTreeUtil.compareTrees(localTreeView, remoteTreeView);
        assertEquals(localTreeView.getNodeHash(0), remoteTreeView.getNodeHash(0));
        assertEquals(localTreeView.getNodeHash(2), remoteTreeView.getNodeHash(2));
        assertEquals(2, deltaOrders.size());
        assertTrue(deltaOrders.containsAll(asList(5, 6)));
    }

    @Test
    public void testSerialization() throws IOException {
        MerkleTree merkleTree = new ArrayMerkleTree(4);
        merkleTree.updateAdd(0x80000000, 1); // leaf 7
        merkleTree.updateAdd(0xA0000000, 2); // leaf 8
        merkleTree.updateAdd(0xC0000000, 3); // leaf 9
        merkleTree.updateAdd(0xE0000000, 4); // leaf 10
        merkleTree.updateAdd(0x00000000, 5); // leaf 11
        merkleTree.updateAdd(0x20000000, 6); // leaf 12
        merkleTree.updateAdd(0x40000000, 7); // leaf 13
        merkleTree.updateAdd(0x60000000, 8); // leaf 14

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(outputStream);
        MerkleTreeUtil.writeLeaves(out, merkleTree);
        byte[] bytes = outputStream.toByteArray();

        InputStream inputStream = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(inputStream);
        RemoteMerkleTreeView remoteMerkleTreeView = MerkleTreeUtil.createRemoteMerkleTreeView(in);

        Collection<Integer> deltaOrders = MerkleTreeUtil.compareTrees(merkleTree, remoteMerkleTreeView);
        assertTrue(deltaOrders.isEmpty());
    }
}
