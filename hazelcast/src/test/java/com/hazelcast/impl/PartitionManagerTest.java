/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.logging.NoLogFactory;
import com.hazelcast.nio.Address;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class PartitionManagerTest {

    @Test
    public void testReArrangeBlocks99_271() throws UnknownHostException {
        testReArrange(99, 271);
    }

    @Test
    public void testReArrangeBlocks8_271() throws UnknownHostException {
        testReArrange(8, 271);
    }

    @Test
    public void testReArrangeBlocks2_271() throws UnknownHostException {
        testReArrange(2, 271);
    }

    @Test
    public void testReArrangeBlocks32_271() throws UnknownHostException {
        testReArrange(32, 271);
    }

    @Test
    public void testReArrangeBlocks1_271() throws UnknownHostException {
        testReArrange(1, 271);
    }

    @Test
    public void testReArrangeBlocks1_999() throws UnknownHostException {
        testReArrange(1, 999);
    }

    @Test
    public void testReArrangeBlocks2_999() throws UnknownHostException {
        testReArrange(2, 999);
    }

    @Test
    public void testReArrangeBlocks29_999() throws UnknownHostException {
        testReArrange(29, 999);
    }

    private void testReArrange(int NODE_COUNT, int BLOCK_COUNT) throws UnknownHostException {
        Address thisAddress = new Address("localhost", 5701);
        testReArrange(NODE_COUNT, BLOCK_COUNT, createBlocks(BLOCK_COUNT, thisAddress));
    }

    private void testReArrange(int NODE_COUNT, int BLOCK_COUNT, Block[] blocks) throws UnknownHostException {
        //setup
        LinkedList<MemberImpl> members = createMembers(NODE_COUNT);
        List<Address> listOfAddresses = getListOfAddresses(members);
        Address thisAddress = new Address("localhost", 5701);
        PartitionManager partitionManager = init(BLOCK_COUNT, blocks, members, thisAddress);
        //run the method
        partitionManager.reArrangeBlocks();
        //assert
        int expected = BLOCK_COUNT * (NODE_COUNT - 1) / NODE_COUNT;
        int found = partitionManager.lsBlocksToMigrate.size();
        assertTrue(found == expected || found == expected + 1);
        for (Block block : blocks) {
            assertEquals(thisAddress, block.getOwner());
        }
        for (Block block : partitionManager.lsBlocksToMigrate) {
            assertTrue(listOfAddresses.contains(block.getMigrationAddress()));
        }
        migrate(blocks, partitionManager.lsBlocksToMigrate);
        partitionManager.lsBlocksToMigrate.clear();
        Map<Address, Integer> counter = new HashMap<Address, Integer>();
        for (int i = 0; i < BLOCK_COUNT; i++) {
            Integer count = counter.get(blocks[i].getOwner());
            count = (count == null) ? 0 : count;
            count++;
            counter.put(blocks[i].getOwner(), count);
        }
        int partitionsPerMember = BLOCK_COUNT / NODE_COUNT;
        for (Address addres : counter.keySet()) {
            int c = counter.get(addres);
            assertTrue(c == partitionsPerMember || c == (partitionsPerMember + 1));
        }
        partitionManager.reArrangeBlocks();
        assertEquals(0, partitionManager.lsBlocksToMigrate.size());
    }

    @Test
    public void testRandomlyAssignBlocksAndRearrange() throws UnknownHostException {
        int blockCount = 271;
        int NODE_COUNT = 0;
        while (NODE_COUNT == 0) {
            NODE_COUNT = (int) (Math.random() * blockCount) / 3;
        }
        LinkedList<MemberImpl> members = createMembers(NODE_COUNT);
        Address thisAddress = new Address("localhost", 5701);
        Block[] blocks = randomlyAssignBlocks(blockCount, members);
        PartitionManager partitionManager = init(271, blocks, members, thisAddress);
        partitionManager.reArrangeBlocks();
        migrate(blocks, partitionManager.lsBlocksToMigrate);
        Map<Address, Integer> counter = countPartitionsPerMember(blocks);
        int partitionsPerMember = blockCount / NODE_COUNT;
        for (Address addres : counter.keySet()) {
            int c = counter.get(addres);
            assertTrue(c == partitionsPerMember || c == (partitionsPerMember + 1));
        }
        partitionManager.reArrangeBlocks();
        migrate(blocks, partitionManager.lsBlocksToMigrate);
        assertEquals(0, partitionManager.lsBlocksToMigrate.size());
    }

    @Test
    public void thousandTimesRandomlyAssignBlocksAndReArrange() throws UnknownHostException {
        for (int i = 0; i < 1000; i++) {
            testRandomlyAssignBlocksAndRearrange();
        }
    }

    private Map<Address, Integer> countPartitionsPerMember(Block[] blocks) {
        Map<Address, Integer> counter = new HashMap<Address, Integer>();
        for (int i = 0; i < blocks.length; i++) {
            Integer count = counter.get(blocks[i].getOwner());
            count = (count == null) ? 0 : count;
            count++;
            counter.put(blocks[i].getOwner(), count);
        }
        return counter;
    }

    private Block[] randomlyAssignBlocks(int blockCount, LinkedList<MemberImpl> members) {
        Block[] blocks = new Block[blockCount];
        int remainingBlocksToDistribute = 271;
        int memberIndex = 0;
        while (remainingBlocksToDistribute != 0) {
            int blocksDistributedSoFar = blockCount - remainingBlocksToDistribute;
            int blocksToDistribute = Math.min((int) (Math.random() * blockCount / members.size() * 2), remainingBlocksToDistribute);
            for (int i = blocksDistributedSoFar; i < blocksDistributedSoFar + blocksToDistribute; i++) {
                Block block = new Block(i, members.get(memberIndex).getAddress());
                blocks[i] = block;
            }
            memberIndex++;
            if (memberIndex == members.size()) {
                memberIndex = 0;
            }
            remainingBlocksToDistribute = remainingBlocksToDistribute - blocksToDistribute;
        }
        return blocks;
    }

    private void migrate(Block[] blocks, List<Block> lsBlocksToMigrate) {
        for (Block blockMig : lsBlocksToMigrate) {
            blocks[blockMig.getBlockId()].setOwner(blockMig.getMigrationAddress());
            blocks[blockMig.getBlockId()].setMigrationAddress(null);
        }
    }

    private List<Address> getListOfAddresses(List<MemberImpl> members) {
        List<Address> listOfAddresses = new LinkedList<Address>();
        for (MemberImpl member : members) {
            listOfAddresses.add(member.getAddress());
        }
        return listOfAddresses;
    }

    private LinkedList<MemberImpl> createMembers(int NODE_COUNT) throws UnknownHostException {
        LinkedList<MemberImpl> members = new LinkedList<MemberImpl>();
        for (int i = 0; i < NODE_COUNT; i++) {
            Address address = new Address("localhost", 5701 + i);
            members.add(new MemberImpl(address, true));
        }
        return members;
    }

    private PartitionManager init(int BLOCK_COUNT, Block[] blocks, LinkedList<MemberImpl> members, Address thisAddress) {
        ConcurrentMapManager concurrentMapManager = mock(ConcurrentMapManager.class);
        Node node = mock(Node.class);
        when(node.getLogger(PartitionManager.class.getName())).thenReturn(new NoLogFactory().getLogger("test"));
        ExecutorManager executorManager = mock(ExecutorManager.class);
        when(node.getExecutorManager()).thenReturn(executorManager);
        when(executorManager.newParallelExecutor(20)).thenReturn(null);
        when(executorManager.newParallelExecutor(12)).thenReturn(null);
        Config config = new Config();
        GroupProperties group = new GroupProperties(config);
        when(node.getGroupProperties()).thenReturn(group);
        when(concurrentMapManager.getNode()).thenReturn(node);
        when(concurrentMapManager.getPartitionCount()).thenReturn(BLOCK_COUNT);
        when(concurrentMapManager.getBlocks()).thenReturn(blocks);
        when(concurrentMapManager.getThisAddress()).thenReturn(thisAddress);
        when(concurrentMapManager.getMembers()).thenReturn(members);
        when(concurrentMapManager.isMaster()).thenReturn(true);
        PartitionManager partitionManager = new PartitionManager(concurrentMapManager);
        return partitionManager;
    }

    private Block[] createBlocks(int blockSize, Address address) throws UnknownHostException {
        Block[] blocks = new Block[blockSize];
        for (int i = 0; i < blockSize; i++) {
            blocks[i] = new Block(i, address);
        }
        return blocks;
    }
}
