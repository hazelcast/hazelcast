/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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

import static com.hazelcast.impl.ClusterOperation.CONCURRENT_MAP_BLOCKS;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MapMigrator implements Runnable {

    final static Logger logger = Logger.getLogger(MapMigrator.class.getName());

    final ConcurrentMapManager concurrentMapManager;
    final Node node;
    final int BLOCK_COUNT;
    final Block[] blocks;
    final Address thisAddress;
    final List<Block> lsBlocksToMigrate = new ArrayList<Block>(100);
    final static long MIGRATION_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(10);

    Block blockMigrating = null;
    long nextMigrationMillis = 0;

    public MapMigrator(ConcurrentMapManager concurrentMapManager) {
        this.concurrentMapManager = concurrentMapManager;
        this.node = concurrentMapManager.node;
        this.BLOCK_COUNT = concurrentMapManager.BLOCK_COUNT;
        this.blocks = concurrentMapManager.blocks;
        this.thisAddress = concurrentMapManager.thisAddress;
    }

    void reArrangeBlocks() {
        if (concurrentMapManager.isMaster()) {
            List<MemberImpl> lsMembers = concurrentMapManager.lsMembers;
            // make sue that all blocks are actually created
            for (int i = 0; i < BLOCK_COUNT; i++) {
                Block block = blocks[i];
                if (block == null) {
                    concurrentMapManager.getOrCreateBlock(i);
                }
            }
            List<Block> lsBlocksToRedistribute = new ArrayList<Block>();
            Map<Address, Integer> addressBlocks = new HashMap<Address, Integer>();
            int storageEnabledMemberCount = 0;
            for (MemberImpl member : lsMembers) {
                if (!member.isSuperClient()) {
                    addressBlocks.put(member.getAddress(), 0);
                    storageEnabledMemberCount++;
                }
            }
            if (storageEnabledMemberCount == 0)
                return;
            int aveBlockOwnCount = BLOCK_COUNT / (storageEnabledMemberCount);
            for (Block block : blocks) {
                if (block.getOwner() == null) {
                    lsBlocksToRedistribute.add(new Block(block));
                } else {
                    if (!block.isMigrating()) {
                        Integer countInt = addressBlocks.get(block.getOwner());
                        int count = (countInt == null) ? 0 : countInt;
                        if (count >= aveBlockOwnCount) {
                            lsBlocksToRedistribute.add(new Block(block));
                        } else {
                            count++;
                            addressBlocks.put(block.getOwner(), count);
                        }
                    }
                }
            }
            Set<Address> allAddress = addressBlocks.keySet();
            lsBlocksToMigrate.clear();
            setNewMembers:
            for (Address address : allAddress) {
                Integer countInt = addressBlocks.get(address);
                int count = (countInt == null) ? 0 : countInt;
                while (count < aveBlockOwnCount) {
                    if (lsBlocksToRedistribute.size() > 0) {
                        Block blockToMigrate = lsBlocksToRedistribute.remove(0);
                        if (blockToMigrate.getOwner() == null) {
                            blockToMigrate.setOwner(address);
                        } else {
                            blockToMigrate.setMigrationAddress(address);
                            if (blockToMigrate.getOwner().equals(blockToMigrate.getMigrationAddress())) {
                                blockToMigrate.setMigrationAddress(null);
                            }
                        }
                        lsBlocksToMigrate.add(blockToMigrate);
                        count++;
                    } else {
                        break setNewMembers;
                    }
                }
            }
            int addressIndex = 0;
            for (int i = 0; i < BLOCK_COUNT; i++) {
                Block block = blocks[i];
                if (block.getOwner() == null) {
                    block = new Block(block);
                    int index = addressIndex++ % addressBlocks.size();
                    block.setOwner((Address) addressBlocks.keySet().toArray()[index]);
                    lsBlocksToRedistribute.add(block);
                }
            }
        }
    }

    public void run() {
        if (!concurrentMapManager.isMaster()) {
            return;
        }
        if (concurrentMapManager.lsMembers.size() < 2) {
            for (int i = 0; i < BLOCK_COUNT; i++) {
                Block block = blocks[i];
                if (block == null) {
                    block = concurrentMapManager.getOrCreateBlock(i);
                    block.setOwner(thisAddress);
                }
            }
            return;
        }
        if (blockMigrating != null) {
            Block blockReal = blocks[blockMigrating.getBlockId()];
            if (blockReal.getOwner() != null && !blockReal.isMigrating()) {
                blockMigrating = null;
                completeMigration();
            }
        }
        if (blockMigrating != null) {
            return;
        }
        if (System.currentTimeMillis() > nextMigrationMillis) {
            for (int i = 0; i < 1; i++) {
                initiateMigration();
            }
        }
    }

    public boolean isMigrating(Request req) {
        if (req.key == null && req.blockId != -1 && concurrentMapManager.hashBlocks() != req.blockId) {
            logger.log(Level.FINEST, thisAddress + " blockHashes aren't the same:"
                    + concurrentMapManager.hashBlocks() + ", request.blockId:"
                    + req.blockId + " caller: " + req.caller);
            return true;
        }
        if (blockMigrating != null) {
            if (!blocks[blockMigrating.getBlockId()].isMigrating()) {
                completeMigration();
            } else {
                if (req.key == null) {
                    return true;
                } else if (blockMigrating.getBlockId() == concurrentMapManager.getBlockId(req.key)) {
                    return true;
                }
            }
        }
        return false;
    }

    void completeMigration() {
        blockMigrating = null;
        nextMigrationMillis = System.currentTimeMillis() + MIGRATION_INTERVAL_MILLIS;
    }

    void initiateMigration() {
//        System.out.println("initiate migration " + lsBlocksToMigrate.size());
        if (lsBlocksToMigrate != null && lsBlocksToMigrate.size() > 0) {
            Block block = lsBlocksToMigrate.remove(0);
            if (thisAddress.equals(block.getOwner())) {
                migrateBlock(block);
            } else {
                concurrentMapManager.sendBlockInfo(block, block.getOwner());
            }
        } else {
            reArrangeBlocks();
//            System.out.println("rearranged " + lsBlocksToMigrate.size());
        }
    }

    public void syncForAdd() {
        if (concurrentMapManager.isMaster()) {
            if (concurrentMapManager.isSuperClient()) {
                MemberImpl nonSuperMember = null;
                for (MemberImpl member : concurrentMapManager.lsMembers) {
                    if (!member.isSuperClient()) {
                        nonSuperMember = member;
                    }
                }
                if (nonSuperMember != null) {
                    for (int i = 0; i < BLOCK_COUNT; i++) {
                        Block block = blocks[i];
                        if (block == null) {
                            block = concurrentMapManager.getOrCreateBlock(i);
                        }
                        if (block.getOwner() == null) {
                            block.setOwner(nonSuperMember.getAddress());
                        }
                    }
                }
            }
            // make sue that all blocks are actually created
            for (int i = 0; i < BLOCK_COUNT; i++) {
                Block block = blocks[i];
                if (block == null) {
                    concurrentMapManager.getOrCreateBlock(i);
                }
            }
            Data dataAllBlocks = null;
            for (MemberImpl member : concurrentMapManager.lsMembers) {
                if (!member.localMember()) {
                    if (dataAllBlocks == null) {
                        ConcurrentMapManager.Blocks allBlocks = new ConcurrentMapManager.Blocks();
                        for (Block block : blocks) {
                            allBlocks.addBlock(block);
                        }
                        dataAllBlocks = ThreadContext.get().toData(allBlocks);
                    }
                    concurrentMapManager.send("blocks", CONCURRENT_MAP_BLOCKS, dataAllBlocks, member.getAddress());
                }
            }
        }
        ConcurrentMapManager.InitialState initialState = new ConcurrentMapManager.InitialState();
        Collection<CMap> cmaps = concurrentMapManager.maps.values();
        for (final CMap cmap : cmaps) {
            initialState.createAndAddMapState(cmap);
        }
        concurrentMapManager.sendProcessableToAll(initialState, false);
        onMembershipChange();
    }

    public void onMembershipChange() {
        lsBlocksToMigrate.clear();
        backupIfNextOrPreviousChanged();
        nextMigrationMillis = System.currentTimeMillis() + MIGRATION_INTERVAL_MILLIS;
    }

    public void syncForDead(Address deadAddress) {
        if (deadAddress == null || deadAddress.equals(thisAddress)) return;
        Set<Integer> blocksOwnedAfterDead = new HashSet<Integer>();
        for (Block block : blocks) {
            if (block != null) {
                if (deadAddress.equals(block.getOwner())) {
                    MemberImpl member = concurrentMapManager.getNextMemberBeforeSync(block.getOwner(), true, 1);
                    if (member == null) {
                        if (!concurrentMapManager.isSuperClient()) {
                            block.setOwner(thisAddress);
                        } else {
                            block.setOwner(null);
                        }
                    } else {
                        if (!deadAddress.equals(member.getAddress())) {
                            block.setOwner(member.getAddress());
                        } else {
                            block.setOwner(null);
                        }
                    }
                    if (thisAddress.equals(block.getOwner())) {
                        blocksOwnedAfterDead.add(block.getBlockId());
                    }
                }
                if (block.getMigrationAddress() != null) {
                    if (deadAddress.equals(block.getMigrationAddress())) {
                        MemberImpl member = concurrentMapManager.getNextMemberBeforeSync(block.getMigrationAddress(), true, 1);
                        if (member == null) {
                            if (!concurrentMapManager.isSuperClient()) {
                                block.setMigrationAddress(thisAddress);
                            } else {
                                block.setMigrationAddress(null);
                            }
                        } else {
                            if (!deadAddress.equals(member.getAddress())) {
                                block.setMigrationAddress(member.getAddress());
                            } else {
                                block.setMigrationAddress(null);
                            }
                        }
                    }
                }
            }
        }
        Collection<CMap> cmaps = concurrentMapManager.maps.values();
        for (CMap map : cmaps) {
            Collection<Record> records = map.mapRecords.values();
            for (Record record : records) {
                if (record != null) {
                    concurrentMapManager.onDisconnect(record, deadAddress);
                    if (record.isActive()) {
                        if (blocksOwnedAfterDead.contains(record.getBlockId())) {
                            map.markAsOwned(record);
                            // you have to update the indexes
                            // as if this record is new so extract the values first
                            int valueHash = record.getValueHash();
                            long[] indexes = record.getIndexes();
                            byte[] indexTypes = record.getIndexTypes();
                            // set the indexes to null, (new record)
                            record.setValueHash(Integer.MIN_VALUE);
                            record.setIndexes(null, null);
                            // now update the index
                            node.queryService.updateIndex(map.name, indexes, indexTypes, record, valueHash);
                        }
                    }
                }
            }
        }
        onMembershipChange();
    }

    void backupIfNextOrPreviousChanged() {
        boolean nextOrPreviousChanged = node.clusterManager.isNextChanged()
                || node.clusterManager.isPreviousChanged();
        if (nextOrPreviousChanged) {
            List<Record> lsOwnedRecords = new ArrayList<Record>(1000);
            Collection<CMap> cmaps = concurrentMapManager.maps.values();
            for (final CMap cmap : cmaps) {
                final Object[] records = cmap.ownedRecords.toArray();
                for (Object recObj : records) {
                    final Record rec = (Record) recObj;
                    if (rec.isActive()) {
                        if (rec.getKey() == null || rec.getKey().size() == 0) {
                            throw new RuntimeException("Record.key is null or empty " + rec.getKey());
                        }
                        lsOwnedRecords.add(rec);
                    }
                }
            }
            for (final Record rec : lsOwnedRecords) {
                concurrentMapManager.executeLocally(new FallThroughRunnable() {
                    public void doRun() {
                        concurrentMapManager.backupRecord(rec);
                    }
                });
            }
        }
    }

    void migrateBlock(final Block block) {
        if (!thisAddress.equals(block.getOwner())) {
            throw new RuntimeException();
        }
        if (block.getMigrationAddress() == null) {
            throw new RuntimeException();
        }
        if (block.getOwner().equals(block.getMigrationAddress())) {
            throw new RuntimeException();
        }
        Block blockReal = blocks[block.getBlockId()];
        blockReal.setOwner(block.getOwner());
        blockReal.setMigrationAddress(block.getMigrationAddress());
        logger.log(Level.FINEST, "migrate block " + block);
        if (!node.active || node.factory.restarted) return;
        if (concurrentMapManager.isSuperClient())
            return;
        blockMigrating = block;
        List<Record> lsRecordsToMigrate = new ArrayList<Record>(1000);
        Collection<CMap> cmaps = concurrentMapManager.maps.values();
        for (final CMap cmap : cmaps) {
            if (cmap.locallyOwnedMap != null) {
                cmap.locallyOwnedMap.reset();
            }
            final Object[] records = cmap.ownedRecords.toArray();
            for (Object recObj : records) {
                final Record rec = (Record) recObj;
                if (rec.isActive()) {
                    if (rec.getKey() == null || rec.getKey().size() == 0) {
                        throw new RuntimeException("Record.key is null or empty " + rec.getKey());
                    }
                    if (rec.getBlockId() == block.getBlockId()) {
                        lsRecordsToMigrate.add(rec);
                        cmap.markAsRemoved(rec);
                    }
                }
            }
        }
        final CountDownLatch latch = new CountDownLatch(lsRecordsToMigrate.size());
        for (final Record rec : lsRecordsToMigrate) {
            final CMap cmap = concurrentMapManager.getMap(rec.getName());
            node.executorManager.executeMigrationTask(new FallThroughRunnable() {
                public void doRun() {
                    try {
//                        System.out.println("migrating.. " + block);
                        concurrentMapManager.migrateRecord(cmap, rec);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        node.executorManager.executeMigrationTask(new FallThroughRunnable() {
            public void doRun() {
                try {
                    latch.await(10, TimeUnit.SECONDS);
                    block.setOwner(block.getMigrationAddress());
                    block.setMigrationAddress(null);
//                System.out.println("migration complete " + block);
                    concurrentMapManager.enqueueAndReturn(new Processable() {
                        public void process() {
                            Block blockReal = blocks[block.getBlockId()];
                            blockReal.setOwner(block.getOwner());
                            blockReal.setMigrationAddress(null);
                        }
                    });
                    for (MemberImpl member : concurrentMapManager.lsMembers) {
                        if (!member.localMember()) {
                            boolean sent = false;
                            while (!sent) {
                                sent = concurrentMapManager.sendBlockInfo(block, member.getAddress());
                                Thread.sleep(1000);
                            }
                        }
                    }
                } catch (InterruptedException ignored) {
                }
            }
        });
    }
}
