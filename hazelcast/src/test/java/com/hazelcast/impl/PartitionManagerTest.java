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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        LinkedList<MemberImpl> members = new LinkedList<MemberImpl>();
        List<Address> listOfAddresses = new ArrayList<Address>();
        for (int i = 0; i < NODE_COUNT; i++) {
            Address address = new Address("localhost", 5701 + i);
            listOfAddresses.add(address);
            members.add(new MemberImpl(address, true));
        }
        Address thisAddress = new Address("localhost", 5701);
        ConcurrentMapManager concurrentMapManager = mock(ConcurrentMapManager.class);
        Node node = mock(Node.class);
        when(node.getLogger(PartitionManager.class.getName())).thenReturn(new NoLogFactory().getLogger("test"));
        Config config = new Config();
        GroupProperties group = new GroupProperties(config);
        when(node.getGroupProperties()).thenReturn(group);
        when(concurrentMapManager.getNode()).thenReturn(node);
        when(concurrentMapManager.getPartitionCount()).thenReturn(BLOCK_COUNT);
        when(concurrentMapManager.getBlocks()).thenReturn(createBlocks(BLOCK_COUNT, thisAddress));
        when(concurrentMapManager.getThisAddress()).thenReturn(thisAddress);
        when(concurrentMapManager.getMembers()).thenReturn(members);
        when(concurrentMapManager.isMaster()).thenReturn(true);
        PartitionManager partitionManager = new PartitionManager(concurrentMapManager);
        partitionManager.reArrangeBlocks();
        int expected = BLOCK_COUNT * (NODE_COUNT - 1) / NODE_COUNT;
        int found = partitionManager.lsBlocksToMigrate.size();
        assertTrue(found == expected || found == expected +1);
        Block[] blocks = concurrentMapManager.getBlocks();
        for (Block block : blocks) {
            assertEquals(thisAddress, block.getOwner());
        }
        for (Block block : partitionManager.lsBlocksToMigrate) {
            assertTrue(listOfAddresses.contains(block.getMigrationAddress()));
        }
        for (Block blockMig : partitionManager.lsBlocksToMigrate) {
            blocks[blockMig.getBlockId()].setOwner(blockMig.getMigrationAddress());
            blocks[blockMig.getBlockId()].setMigrationAddress(null);
        }
        partitionManager.lsBlocksToMigrate.clear();
        partitionManager.reArrangeBlocks();
        assertEquals(0, partitionManager.lsBlocksToMigrate.size());
    }

    private Block[] createBlocks(int blockSize, Address address) throws UnknownHostException {
        Block[] blocks = new Block[blockSize];
        for (int i = 0; i < blockSize; i++) {
            blocks[i] = new Block(i, address);
        }
        return blocks;
    }
}
