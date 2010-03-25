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

import com.hazelcast.core.Member;
import com.hazelcast.impl.concurrentmap.InitialState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.partition.MigrationEvent;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.impl.ClusterOperation.CONCURRENT_MAP_BLOCKS;

public class PartitionManager implements Runnable {

    final ILogger logger;

    final ConcurrentMapManager concurrentMapManager;
    final PartitionServiceImpl partitionServiceImpl;
    final Node node;
    final int PARTITION_COUNT;
    final Block[] blocks;
    final Address thisAddress;
    final List<Block> lsBlocksToMigrate = new ArrayList<Block>(100);

    final long MIGRATION_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(10);

    long nextMigrationMillis = System.currentTimeMillis() + MIGRATION_INTERVAL_MILLIS;

    boolean dirty = false;

    public PartitionManager(ConcurrentMapManager concurrentMapManager) {
        this.logger = concurrentMapManager.getNode().getLogger(PartitionManager.class.getName());
        this.concurrentMapManager = concurrentMapManager;
        this.node = concurrentMapManager.getNode();
        this.PARTITION_COUNT = concurrentMapManager.getPartitionCount();
        this.blocks = concurrentMapManager.getBlocks();
        this.thisAddress = concurrentMapManager.getThisAddress();
        this.partitionServiceImpl = new PartitionServiceImpl(concurrentMapManager);
    }

    public void run() {
        long now = System.currentTimeMillis();
        if (now > nextMigrationMillis) {
            nextMigrationMillis = now + MIGRATION_INTERVAL_MILLIS;
            if (!concurrentMapManager.isMaster()) return;
            if (concurrentMapManager.getMembers().size() < 2) return;
            initiateMigration();
        }
    }

    void reArrangeBlocks() {
        if (concurrentMapManager.isMaster()) {
            Map<Address, Integer> addressBlocks = getCurrentMemberBlocks();
            if (addressBlocks.size() == 0) {
                return;
            }
            List<Block> lsBlocksToRedistribute = new ArrayList<Block>();
            int aveBlockOwnCount = PARTITION_COUNT / (addressBlocks.size());
            for (Block blockReal : blocks) {
                if (!blockReal.isMigrating()) {
                    Integer countInt = addressBlocks.get(blockReal.getOwner());
                    int count = (countInt == null) ? 0 : countInt;
                    if (count >= aveBlockOwnCount) {
                        lsBlocksToRedistribute.add(new Block(blockReal));
                    } else {
                        addressBlocks.put(blockReal.getOwner(), ++count);
                    }
                }
            }
            lsBlocksToMigrate.clear();
            for (Address address : addressBlocks.keySet()) {
                Integer countInt = addressBlocks.get(address);
                int count = (countInt == null) ? 0 : countInt;
                while (count < aveBlockOwnCount && lsBlocksToRedistribute.size() > 0) {
                    Block blockToMigrate = lsBlocksToRedistribute.remove(0);
                    addBlockToMigrate(blockToMigrate, address);
                    count++;
                }
            }
            lsBlocksToRedistribute.removeAll(lsBlocksToMigrate);
            for (MemberImpl member : concurrentMapManager.getMembers()) {
                if (lsBlocksToRedistribute.size() == 0) {
                    break;
                }
                Block blockToMigrate = lsBlocksToRedistribute.remove(0);
                addBlockToMigrate(blockToMigrate, member.getAddress());
            }
            Collections.shuffle(lsBlocksToMigrate);
        }
    }

    private void addBlockToMigrate(Block blockToMigrate, Address address) {
        if (!blockToMigrate.getOwner().equals(address)) {
            blockToMigrate.setMigrationAddress(address);
            lsBlocksToMigrate.add(blockToMigrate);
        }
    }

    boolean isBlockEmpty(int blockId) {
        Collection<CMap> cmaps = concurrentMapManager.maps.values();
        for (final CMap cmap : cmaps) {
            if (cmap.hasOwned(blockId)) {
                return false;
            }
        }
        return true;
    }

    public boolean containsMigratingBlock() {
        for (Block block : blocks) {
            if (block != null && block.isMigrating()) {
                return true;
            }
        }
        return false;
    }

    public boolean isMigrating(Request req) {
        if (req.key == null && req.blockId != -1 && concurrentMapManager.hashBlocks() != req.blockId) {
            logger.log(Level.FINEST, thisAddress + " blockHashes aren't the same:"
                    + concurrentMapManager.hashBlocks() + ", request.blockId:"
                    + req.blockId + " caller: " + req.caller);
            return true;
        }
        if (req.key != null) {
            Block block = concurrentMapManager.getOrCreateBlock(req.key);
            return block.isMigrating();
        } else {
            for (Block block : blocks) {
                if (block != null && block.isMigrating()) return true;
            }
        }
        return false;
    }

    void initiateMigration() {
        boolean hasMigrating = false;
        for (int i = 0; i < PARTITION_COUNT; i++) {
            Block block = blocks[i];
            if (block == null) {
                block = concurrentMapManager.getOrCreateBlock(i);
                block.setOwner(thisAddress);
            }
            if (!hasMigrating && block.isMigrating()) {
                hasMigrating = true;
            }
        }
        if (lsBlocksToMigrate.size() == 0) {
            reArrangeBlocks();
        }
        if (!hasMigrating && lsBlocksToMigrate.size() > 0) {
            Block block = lsBlocksToMigrate.remove(0);
            sendBlocks(block);
        } else {
            sendBlocks(null);
        }
    }

    public void syncForAdd() {
        if (concurrentMapManager.isMaster()) {
            if (concurrentMapManager.isSuperClient()) {
                MemberImpl nonSuperMember = null;
                for (MemberImpl member : concurrentMapManager.getMembers()) {
                    if (!member.isSuperClient()) {
                        nonSuperMember = member;
                    }
                }
                if (nonSuperMember != null) {
                    for (int i = 0; i < PARTITION_COUNT; i++) {
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
            // make sure that all blocks are actually created
            for (int i = 0; i < PARTITION_COUNT; i++) {
                Block block = blocks[i];
                if (block == null) {
                    block = concurrentMapManager.getOrCreateBlock(i);
                }
            }
            if (node.groupProperties.INITIAL_WAIT_SECONDS.getInteger() == 0) {
                quickBlockRearrangement();
            } else {
                createAllBlocks();
                sendBlocks(null);
            }
        }
        InitialState initialState = new InitialState();
        Collection<CMap> cmaps = concurrentMapManager.maps.values();
        for (final CMap cmap : cmaps) {
            initialState.createAndAddMapState(cmap);
        }
        concurrentMapManager.sendProcessableToAll(initialState, false);
        onMembershipChange(true);
    }

    Map<Address, Integer> getCurrentMemberBlocks() {
        List<MemberImpl> lsMembers = concurrentMapManager.getMembers();
        Map<Address, Integer> addressBlocks = new HashMap<Address, Integer>();
        for (MemberImpl member : lsMembers) {
            if (!member.isSuperClient()) {
                addressBlocks.put(member.getAddress(), 0);
            }
        }
        return addressBlocks;
    }

    void sendBlocks(Block blockInfo) {
        if (blockInfo != null && blockInfo.isMigrating() && blockInfo.getMigrationAddress().equals(blockInfo.getOwner())) {
            blockInfo.setMigrationAddress(null);
        }
        for (int i = 0; i < PARTITION_COUNT; i++) {
            Block block = blocks[i];
            if (block == null) {
                block = concurrentMapManager.getOrCreateBlock(i);
            }
            if (block.getOwner() == null && !concurrentMapManager.isSuperClient()) {
                block.setOwner(thisAddress);
            } else if (block.getOwner() != null && block.getOwner().equals(block.getMigrationAddress())) {
                block.setMigrationAddress(null);
            }
        }
        Blocks allBlockOwners = new Blocks();
        for (Block block : blocks) {
            if (blockInfo != null && block.getBlockId() == blockInfo.getBlockId()) {
                allBlockOwners.addBlock(blockInfo);
            } else {
                allBlockOwners.addBlock(block);
            }
        }
        allBlockOwners.setNode(node);
        Data dataAllBlocks = ThreadContext.get().toData(allBlockOwners);
        for (MemberImpl member : concurrentMapManager.getMembers()) {
            if (!member.localMember()) {
                concurrentMapManager.send("blocks", CONCURRENT_MAP_BLOCKS, dataAllBlocks, member.getAddress());
            } else {
                allBlockOwners.process();
            }
        }
    }

    void quickBlockRearrangement() {
        if (!concurrentMapManager.isMaster()) return;
        createAllBlocks();
        // find storage enabled members
        Map<Address, Integer> addressBlocks = getCurrentMemberBlocks();
        if (addressBlocks.size() == 0) {
            return;
        }
        // find unowned blocks
        // find owned by me but empty blocks
        // distribute them first
        // equally redistribute again
        List<Block> ownableBlocks = new ArrayList<Block>();
        for (Block blockReal : blocks) {
            if (blockReal.getOwner() == null) {  // in case master is superClient
                ownableBlocks.add(blockReal);
            } else if (!blockReal.isMigrating() && thisAddress.equals(blockReal.getOwner())) {
                // i am the owner
                // is it empty? can I give that block
                // without migrating
                if (isBlockEmpty(blockReal.getBlockId())) {
                    ownableBlocks.add(blockReal);
                }
            }
            Integer countInt = addressBlocks.get(blockReal.getOwner());
            int count = (countInt == null) ? 0 : countInt;
            addressBlocks.put(blockReal.getOwner(), ++count);
        }
        int aveBlockOwnCount = PARTITION_COUNT / (addressBlocks.size());
        Set<Address> allAddress = addressBlocks.keySet();
        for (Address address : allAddress) {
            Integer countInt = addressBlocks.get(address);
            int count = (countInt == null) ? 0 : countInt;
            while (count < aveBlockOwnCount && ownableBlocks.size() > 0) {
                Block ownableBlock = ownableBlocks.remove(0);
                ownableBlock.setOwner(address);
                count++;
            }
        }
        sendBlocks(null);
    }

    private void createAllBlocks() {
        //create all blocks
        for (int i = 0; i < PARTITION_COUNT; i++) {
            Block block = blocks[i];
            if (block == null) {
                block = concurrentMapManager.getOrCreateBlock(i);
            }
            if (block.getOwner() == null && !concurrentMapManager.isSuperClient()) {
                block.setOwner(thisAddress);
            }
        }
    }

    public void onMembershipChange(boolean add) {
        lsBlocksToMigrate.clear();
        backupIfNextOrPreviousChanged(add);
        if (concurrentMapManager.isMaster()) {
            sendBlocks(null);
        }
        nextMigrationMillis = System.currentTimeMillis() + MIGRATION_INTERVAL_MILLIS;
    }

    public void syncForDead(MemberImpl deadMember) {
        Address deadAddress = deadMember.getAddress();
        if (deadAddress == null || deadAddress.equals(thisAddress)) {
            return;
        }
        Set<Integer> blocksOwnedAfterDead = new HashSet<Integer>();
        for (Block block : blocks) {
            if (block != null) {
                syncForDead(deadMember, block);
                if (thisAddress.equals(block.getOwner())) {
                    blocksOwnedAfterDead.add(block.getBlockId());
                }
            }
        }
        Collection<CMap> cmaps = concurrentMapManager.maps.values();
        for (CMap cmap : cmaps) {
            Object[] records = cmap.mapRecords.values().toArray();
            for (Object recordObject : records) {
                if (recordObject != null) {
                    Record record = (Record) recordObject;
                    cmap.onDisconnect(record, deadAddress);
                    if (record.isActive() && blocksOwnedAfterDead.contains(record.getBlockId())) {
                        // you have to update the indexes
                        cmap.updateIndexes(record);
                    }
                }
            }
        }
        onMembershipChange(false);
    }

    void syncForDead(MemberImpl deadMember, Block block) {
        Address deadAddress = deadMember.getAddress();
        if (deadAddress.equals(block.getOwner())) {
            MemberImpl member = concurrentMapManager.getNextMemberBeforeSync(deadAddress, true, 1);
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
            if (!block.isMigrating()) {
                Member currentOwner = (block.getOwner() == null) ? null : concurrentMapManager.getMember(block.getOwner());
                if (currentOwner != null) {
                    MigrationEvent migrationEvent = new MigrationEvent(concurrentMapManager.node,
                            block.getBlockId(), deadMember, currentOwner);
                    partitionServiceImpl.doFireMigrationEvent(true, migrationEvent);
                    partitionServiceImpl.doFireMigrationEvent(false, migrationEvent);
                }
            }
        }
        if (block.isMigrating()) {
            if (deadAddress.equals(block.getMigrationAddress())) {
                MemberImpl member = concurrentMapManager.getNextMemberBeforeSync(deadAddress, true, 1);
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
        for (Block b : blocks) {
            if (b != null && b.isMigrating() && b.getMigrationAddress().equals(b.getOwner())) {
                b.setMigrationAddress(null);
            }
        }
    }

    void backupIfNextOrPreviousChanged(boolean add) {
        boolean shouldBackup = false;
        if (add && node.clusterManager.isNextChanged()) {
            shouldBackup = true;
        } else if (!add && node.clusterManager.isPreviousChanged()) {
            shouldBackup = true;
        }
        //todo  should we check member size. if <=1 we should skip this
        if (shouldBackup) {
            List<Record> lsOwnedRecords = new ArrayList<Record>(1000);
            Collection<CMap> cmaps = concurrentMapManager.maps.values();
            for (final CMap cmap : cmaps) {
                for (Record rec : cmap.mapRecords.values()) {
                    if (rec.isActive()) {
                        if (rec.getKey() == null || rec.getKey().size() == 0) {
                            throw new RuntimeException("Record.key is null or empty " + rec.getKey());
                        }
                        lsOwnedRecords.add(rec);
                    }
                }
            }
            for (final Record rec : lsOwnedRecords) {
                node.executorManager.executeLocally(new FallThroughRunnable() {
                    public void doRun() {
                        concurrentMapManager.backupRecord(rec);
                    }
                });
            }
        }
    }

    void migrateBlock(final Block blockInfo) {
        Block blockReal = blocks[blockInfo.getBlockId()];
        if (!thisAddress.equals(blockInfo.getOwner())) {
            throw new RuntimeException("migrateBlock should be called from owner: " + blockInfo);
        }
        if (!blockInfo.isMigrating()) {
            throw new RuntimeException("migrateBlock cannot have non-migrating block: " + blockInfo);
        }
        if (blockInfo.getOwner().equals(blockInfo.getMigrationAddress())) {
            throw new RuntimeException("migrateBlock cannot have same owner and migrationAddress:" + blockInfo);
        }
        if (!node.isActive() || node.factory.restarted) {
            return;
        }
        if (concurrentMapManager.isSuperClient()) {
            return;
        }
        if (blockReal.isMigrationStarted()) return;
        blockReal.setMigrationStarted(true);
        blockReal.setOwner(blockInfo.getOwner());
        blockReal.setMigrationAddress(blockInfo.getMigrationAddress());
        logger.log(Level.FINEST, "migrate blockInfo " + blockInfo);
        List<Record> lsRecordsToMigrate = new ArrayList<Record>(1000);
        Collection<CMap> cmaps = concurrentMapManager.maps.values();
        for (final CMap cmap : cmaps) {
            if (cmap.locallyOwnedMap != null) {
                cmap.locallyOwnedMap.reset();
            }
            for (Record rec : cmap.mapRecords.values()) {
                if (rec.isActive() && concurrentMapManager.isOwned(rec)) {
                    if (rec.getKey() == null || rec.getKey().size() == 0) {
                        throw new RuntimeException("Record.key is null or empty " + rec.getKey());
                    }
                    if (rec.getBlockId() == blockInfo.getBlockId()) {
                        Record recordCopy = rec.copy();
                        lsRecordsToMigrate.add(recordCopy);
                        cmap.markAsRemoved(rec);
                    }
                }
            }
        }
        logger.log(Level.FINEST, "Migrating [" + lsRecordsToMigrate.size() + "] " + blockInfo);
        final CountDownLatch latch = new CountDownLatch(lsRecordsToMigrate.size());
        for (final Record rec : lsRecordsToMigrate) {
            final CMap cmap = concurrentMapManager.getMap(rec.getName());
            node.executorManager.executeMigrationTask(new FallThroughRunnable() {
                public void doRun() {
                    try {
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
                    logger.log(Level.FINEST, "migrate blockInfo " + blockInfo + " await ");
                    latch.await(10, TimeUnit.SECONDS);
                    concurrentMapManager.enqueueAndReturn(new Processable() {
                        public void process() {
                            blockInfo.setOwner(blockInfo.getMigrationAddress());
                            blockInfo.setMigrationAddress(null);
                            completeMigration(blockInfo.getBlockId());
                            sendCompletionInfo(blockInfo);
                        }
                    });
                } catch (InterruptedException ignored) {
                }
            }
        });
    }

    void sendCompletionInfo(Block blockInfo) {
        for (MemberImpl member : concurrentMapManager.lsMembers) {
            if (!member.localMember()) {
                concurrentMapManager.sendBlockInfo(new Block(blockInfo), member.getAddress());
            }
        }
    }

    void completeMigration(int blockId) {
        dirty = true;
        Block blockReal = blocks[blockId];
        if (blockReal != null && blockReal.isMigrating()) {
            fireMigrationEvent(false, new Block(blockReal));
            blockReal.setOwner(blockReal.getMigrationAddress());
            blockReal.setMigrationAddress(null);
            logger.log(Level.FINEST, "Migration complete info : " + blockReal);
            nextMigrationMillis = System.currentTimeMillis() + MIGRATION_INTERVAL_MILLIS;
            Collection<CMap> cmaps = concurrentMapManager.maps.values();
            for (final CMap cmap : cmaps) {
                cmap.resetLocalMapStats();
            }
        }
    }

    void handleBlocks(Blocks blockOwners) {
        List<Block> lsBlocks = blockOwners.lsBlocks;
        for (Block block : lsBlocks) {
            Block blockReal = concurrentMapManager.getOrCreateBlock(block.getBlockId());
            if (!sameBlocks(block, blockReal)) {
                if (blockReal.getOwner() == null) {
                    blockReal.setOwner(block.getOwner());
                }
                if (thisAddress.equals(block.getOwner()) && block.isMigrating()) {
                    startMigration(new Block(block));
                } else {
                    blockReal.setOwner(block.getOwner());
                    blockReal.setMigrationAddress(block.getMigrationAddress());
                    if (block.isMigrating()) {
                        fireMigrationEvent(true, new Block(block));
                    }
                }
            }
            if (!sameBlocks(block, blockReal)) {
                logger.log(Level.SEVERE, blockReal + " Still blocks don't match " + block);
            }
        }
    }

    void startMigration(Block block) {
        logger.log(Level.FINEST, "Migration Started " + block);
        fireMigrationEvent(true, new Block(block));
        migrateBlock(block);
    }

    boolean sameBlocks(Block b1, Block b2) {
        if (b1.getBlockId() != b2.getBlockId()) {
            throw new IllegalArgumentException("Not the same blocks!");
        }
        if (b1.getOwner() == null) {
            if (b2.getOwner() == null) {
                return true;
            }
        }
        if (!b1.getOwner().equals(b2.getOwner())) {
            return false;
        }
        if (b1.isMigrating() && !b1.getMigrationAddress().equals(b2.getMigrationAddress())) {
            return false;
        }
        return true;
    }

    void fireMigrationEvent(final boolean started, Block block) {
        final MemberImpl memberOwner = concurrentMapManager.getMember(block.getOwner());
        final MemberImpl memberMigration = concurrentMapManager.getMember(block.getMigrationAddress());
        final MigrationEvent migrationEvent = new MigrationEvent(concurrentMapManager.node, block.getBlockId(), memberOwner, memberMigration);
        partitionServiceImpl.doFireMigrationEvent(started, migrationEvent);
    }
}
