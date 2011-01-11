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
import com.hazelcast.impl.executor.ParallelExecutor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.partition.MigrationEvent;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.impl.ClusterOperation.CONCURRENT_MAP_BLOCKS;

public class PartitionManager implements Runnable {

    final int PARTITION_COUNT;
    final long MIGRATION_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(10);

    final ILogger logger;
    final ConcurrentMapManager concurrentMapManager;
    final PartitionServiceImpl partitionServiceImpl;
    final Node node;
    final Block[] blocks;
    final Address thisAddress;
    final List<Block> lsBlocksToMigrate = new ArrayList<Block>(100);
    final ParallelExecutor parallelExecutorMigration;
    final ParallelExecutor parallelExecutorBackups;
    final long timeToInitiateMigration;

    long nextMigrationMillis = System.currentTimeMillis() + MIGRATION_INTERVAL_MILLIS;
    long migrationStartTime = 0;
    int MIGRATION_COMPLETE_WAIT_SECONDS = 0;
    int blocksHash = Integer.MIN_VALUE;

    public PartitionManager(ConcurrentMapManager concurrentMapManager) {
        this.logger = concurrentMapManager.getNode().getLogger(PartitionManager.class.getName());
        this.concurrentMapManager = concurrentMapManager;
        this.node = concurrentMapManager.getNode();
        this.PARTITION_COUNT = concurrentMapManager.getPartitionCount();
        this.blocks = concurrentMapManager.getBlocks();
        this.thisAddress = concurrentMapManager.getThisAddress();
        this.partitionServiceImpl = new PartitionServiceImpl(concurrentMapManager);
        this.timeToInitiateMigration = System.currentTimeMillis() + node.getGroupProperties().INITIAL_WAIT_SECONDS.getInteger() * 1000 + MIGRATION_INTERVAL_MILLIS;
        this.parallelExecutorMigration = node.getExecutorManager().newParallelExecutor(node.getGroupProperties().EXECUTOR_MIGRATION_THREAD_COUNT.getInteger());
        this.parallelExecutorBackups = node.getExecutorManager().newParallelExecutor(20);
    }

    public void reset() {
        lsBlocksToMigrate.clear();
        for (int i = 0; i < PARTITION_COUNT; i++) {
            blocks[i] = null;
        }
        partitionServiceImpl.reset();
        parallelExecutorBackups.shutdown();
        parallelExecutorMigration.shutdown();
    }

    public void run() {
        long now = System.currentTimeMillis();
        if (now > nextMigrationMillis) {
            if (migrationStartTime != 0) {
                logger.log(Level.WARNING,
                        "Migration is not completed for " + ((now - migrationStartTime) / 1000) + " seconds.");
                for (Block block : blocks) {
                    logger.log(Level.WARNING, block == null ? "null" : block.toString());
                }
            }
            nextMigrationMillis = now + MIGRATION_INTERVAL_MILLIS;
            if (!concurrentMapManager.isMaster()) return;
            if (concurrentMapManager.getMembers().size() < 2) return;
            if (now > timeToInitiateMigration) {
                initiateMigration();
            }
        }
    }

    void reArrangeBlocks() {
        if (concurrentMapManager.isMaster()) {
            //there should be no one migrating...
            Map<Address, List<Block>> addressBlocks = getCurrentMemberBlocks();
            if (addressBlocks.size() == 0) {
                return;
            }
            List<Block> lsBlocksToRedistribute = new ArrayList<Block>();
            int aveBlockOwnCount = PARTITION_COUNT / (addressBlocks.size());
            int membersWithMorePartitionsThanAverage = PARTITION_COUNT - addressBlocks.keySet().size() * aveBlockOwnCount;
            for (Address address : addressBlocks.keySet()) {
                List<Block> blocks = addressBlocks.get(address);
                if (membersWithMorePartitionsThanAverage != 0 && blocks.size() == aveBlockOwnCount + 1) {
                    membersWithMorePartitionsThanAverage--;
                    continue;
                }
                int diff = (blocks.size() - aveBlockOwnCount);
                for (int i = 0; i < diff; i++) {
                    Block block = blocks.remove(0);
                    lsBlocksToRedistribute.add(block);
                }
            }
            lsBlocksToMigrate.clear();
            for (Address address : addressBlocks.keySet()) {
                List<Block> blocks = addressBlocks.get(address);
                int count = blocks.size();
                while (count < aveBlockOwnCount && lsBlocksToRedistribute.size() > 0) {
                    Block blockToMigrate = lsBlocksToRedistribute.remove(0);
                    addBlockToMigrate(blockToMigrate, address);
                    count++;
                }
            }
            lsBlocksToRedistribute.removeAll(lsBlocksToMigrate);
            for (Address address : addressBlocks.keySet()) {
                if (lsBlocksToRedistribute.size() == 0) {
                    break;
                }
                if (addressBlocks.get(address).size() != aveBlockOwnCount + 1) {
                    Block blockToMigrate = lsBlocksToRedistribute.remove(0);
                    addBlockToMigrate(blockToMigrate, address);
                }
            }
            Collections.shuffle(lsBlocksToMigrate);
        }
    }

    void quickBlockRearrangement() {
        if (!concurrentMapManager.isMaster()) return;
        createAllBlocks();
        // find storage enabled members
        Map<Address, List<Block>> addressBlocks = getCurrentMemberBlocks();
        if (addressBlocks.size() == 0) {
            return;
        }
        // find unowned blocks
        // find owned by me but empty blocks
        // distribute them first
        // equally redistribute again
        List<Block> lsEmptyBlocks = new ArrayList<Block>();
        for (Block blockReal : blocks) {
            if (!blockReal.isMigrating() && thisAddress.equals(blockReal.getOwner())) {
                // i am the owner
                // is it empty? can I give that block
                // without migrating
                if (isBlockEmpty(blockReal.getBlockId())) {
                    lsEmptyBlocks.add(blockReal);
                }
            }
        }
        int aveBlockOwnCount = PARTITION_COUNT / (addressBlocks.size());
        for (final Entry<Address, List<Block>> entry : addressBlocks.entrySet()) {
            final Address address = entry.getKey();
            List<Block> blocks = entry.getValue();
            int diff = (aveBlockOwnCount - blocks.size());
            for (int i = 0; i < diff && lsEmptyBlocks.size() > 0; i++) {
                Block block = lsEmptyBlocks.remove(0);
                block.setOwner(address);
            }
        }
        if (lsEmptyBlocks.size() > 0) {
            for (Address address : addressBlocks.keySet()) {
                if (lsEmptyBlocks.size() == 0) {
                    break;
                }
                Block ownableBlock = lsEmptyBlocks.remove(0);
                ownableBlock.setOwner(address);
            }
        }
        sendBlocks(null);
        partitionServiceImpl.reset();
    }

    private void createAllBlocks() {
        //create all blocks
        for (int i = 0; i < PARTITION_COUNT; i++) {
            Block block = blocks[i];
            if (block == null) {
                block = getOrCreateBlock(i);
            }
            if (block.getOwner() == null && !concurrentMapManager.isSuperClient()) {
                block.setOwner(thisAddress);
            }
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
        if (req.key == null && req.blockId != -1 && hashBlocks() != req.blockId) {
            logger.log(Level.FINEST, thisAddress + " blockHashes aren't the same:"
                    + hashBlocks() + ", request.blockId:"
                    + req.blockId + " caller: " + req.caller);
            invalidateBlocksHash();
            return true;
        }
        if (req.key != null) {
            Block block = concurrentMapManager.getOrCreateBlock(req);
            return block.isMigrating();
        } else {
            return containsMigratingBlock();
        }
    }

    void initiateMigration() {
        boolean hasMigrating = false;
        for (int i = 0; i < PARTITION_COUNT; i++) {
            Block block = blocks[i];
            if (block == null) {
                block = getOrCreateBlock(i);
            }
            if (block.isMigrating()) {
                hasMigrating = true;
            }
        }
        if (!hasMigrating && lsBlocksToMigrate.size() == 0) {
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
        invalidateBlocksHash();
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
                            block = getOrCreateBlock(i);
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
                    block = getOrCreateBlock(i);
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

    Map<Address, List<Block>> getCurrentMemberBlocks() {
        Map<Address, List<Block>> addressBlocks = new HashMap<Address, List<Block>>();
        List<MemberImpl> lsMembers = concurrentMapManager.getMembers();
        for (MemberImpl member : lsMembers) {
            if (!member.isSuperClient()) {
                addressBlocks.put(member.getAddress(), new ArrayList<Block>());
            }
        }
        if (addressBlocks.size() > 0) {
            for (Block blockReal : blocks) {
                if (blockReal != null && !blockReal.isMigrating()) {
                    List<Block> ownedBlocks = addressBlocks.get(blockReal.getOwner());
                    if (ownedBlocks != null) {
                        ownedBlocks.add(new Block(blockReal));
                    }
                }
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
                block = getOrCreateBlock(i);
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

    public void onMembershipChange(boolean add) {
        lsBlocksToMigrate.clear();
        backupIfNextOrPreviousChanged(add);
        if (concurrentMapManager.isMaster()) {
            sendBlocks(null);
        }
        nextMigrationMillis = System.currentTimeMillis() + MIGRATION_INTERVAL_MILLIS;
    }

    public void syncForDead(MemberImpl deadMember) {
        invalidateBlocksHash();
        partitionServiceImpl.reset();
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
                        cmap.markAsDirty(record);
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
        if (block.isMigrating() && block.getMigrationAddress().equals(block.getOwner())) {
            block.setMigrationAddress(null);
            Member currentOwner = (block.getOwner() == null) ? null : concurrentMapManager.getMember(block.getOwner());
            MigrationEvent migrationEvent = new MigrationEvent(concurrentMapManager.node,
                    block.getBlockId(), deadMember, currentOwner);
            partitionServiceImpl.doFireMigrationEvent(true, migrationEvent);
            partitionServiceImpl.doFireMigrationEvent(false, migrationEvent);
        }
        partitionServiceImpl.reset();
    }

    void backupIfNextOrPreviousChanged(boolean add) {
        List<Record> lsOwnedRecords = new ArrayList<Record>(1000);
        Collection<CMap> cmaps = concurrentMapManager.maps.values();
        for (final CMap cmap : cmaps) {
            boolean shouldBackup = false;
            if (cmap.backupCount > 0) {
                if (add) {
                    shouldBackup = node.clusterManager.isNextChanged(cmap.backupCount);
                } else {
                    shouldBackup = node.clusterManager.isNextChanged(cmap.backupCount) || node.clusterManager.isPreviousChanged(cmap.backupCount);
                }
            }
            if (shouldBackup) {
                for (Record rec : cmap.mapRecords.values()) {
                    if (rec.isActive()) {
                        if (rec.getKeyData() == null || rec.getKeyData().size() == 0) {
                            throw new RuntimeException("Record.key is null or empty " + rec.getKeyData());
                        }
                        lsOwnedRecords.add(rec);
                    }
                }
            }
        }
        if (!add) logger.log(Level.FINEST, thisAddress + " will backup " + lsOwnedRecords.size());
        for (final Record rec : lsOwnedRecords) {
            parallelExecutorBackups.execute(new FallThroughRunnable() {
                public void doRun() {
                    concurrentMapManager.backupRecord(rec);
                }
            });
        }
    }

    void migrateBlock(final Block blockInfo) {
        Block blockReal = blocks[blockInfo.getBlockId()];
        if (!thisAddress.equals(blockInfo.getOwner())) {
            throw new RuntimeException(thisAddress + ". migrateBlock should be called from owner: " + blockInfo);
        }
        if (!blockInfo.isMigrating()) {
            throw new RuntimeException(thisAddress + ". migrateBlock cannot have non-migrating block: " + blockInfo);
        }
        if (blockInfo.getOwner().equals(blockInfo.getMigrationAddress())) {
            throw new RuntimeException(thisAddress + ". migrateBlock cannot have same owner and migrationAddress:" + blockInfo);
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
            for (Record rec : cmap.mapRecords.values()) {
                if (rec.isActive() && concurrentMapManager.isOwned(rec)) {
                    if (rec.getKeyData() == null || rec.getKeyData().size() == 0) {
                        throw new RuntimeException("Record.key is null or empty " + rec.getKeyData());
                    }
                    if (rec.getBlockId() == blockInfo.getBlockId()) {
                        cmap.onMigrate(rec);
                        Record recordCopy = rec.copy();
                        lsRecordsToMigrate.add(recordCopy);
                        cmap.markAsEvicted(rec);
                    }
                }
            }
        }
        logger.log(Level.FINEST, "Migrating [" + lsRecordsToMigrate.size() + "] " + blockInfo);
        final CountDownLatch latch = new CountDownLatch(lsRecordsToMigrate.size());
        for (final Record rec : lsRecordsToMigrate) {
            final CMap cmap = concurrentMapManager.getMap(rec.getName());
            parallelExecutorMigration.execute(new FallThroughRunnable() {
                public void doRun() {
                    try {
                        concurrentMapManager.migrateRecord(cmap, rec);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        parallelExecutorMigration.execute(new FallThroughRunnable() {
            public void doRun() {
                try {
                    logger.log(Level.FINEST, "migrate blockInfo " + blockInfo + " await ");
                    latch.await(10, TimeUnit.SECONDS);
                    if (MIGRATION_COMPLETE_WAIT_SECONDS > 0) {
                        Thread.sleep(MIGRATION_COMPLETE_WAIT_SECONDS * 1000);
                    }
                } catch (InterruptedException ignored) {
                } finally {
                    concurrentMapManager.enqueueAndReturn(new Processable() {
                        public void process() {
                            blockInfo.setOwner(blockInfo.getMigrationAddress());
                            blockInfo.setMigrationAddress(null);
                            completeMigration(blockInfo.getBlockId());
                            sendCompletionInfo(blockInfo);
                        }
                    });
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
        invalidateBlocksHash();
        Block blockReal = blocks[blockId];
        if (blockReal != null && blockReal.isMigrating()) {
            fireMigrationEvent(false, new Block(blockReal));
            blockReal.setOwner(blockReal.getMigrationAddress());
            blockReal.setMigrationAddress(null);
            logger.log(Level.FINEST, "Migration complete info : " + blockReal);
            nextMigrationMillis = System.currentTimeMillis() + MIGRATION_INTERVAL_MILLIS;
        }
    }

    void handleBlocks(Blocks blockOwners) {
        for (Block block : blockOwners.lsBlocks) {
            Block blockReal = getOrCreateBlock(block.getBlockId());
            boolean same = sameBlocks(block, blockReal);
            blockReal.setOwner(block.getOwner());
            blockReal.setMigrationAddress(block.getMigrationAddress());
            if (thisAddress.equals(blockReal.getOwner()) && blockReal.isMigrating()) {
                startMigration(new Block(block));
            } else if (!same && block.getOwner() != null) {
                boolean started = blockReal.isMigrating();
                if (started) {
                    fireMigrationEvent(started, new Block(block));
                } else {
                    fireMigrationEvent(started, new Block(block.getBlockId(), null, block.getOwner()));
                }
            }
            if (!same) {
                invalidateBlocksHash();
            }
        }
    }

    Block getOrCreateBlock(int blockId) {
        concurrentMapManager.checkServiceThread();
        Block block = blocks[blockId];
        if (block == null) {
            block = new Block(blockId, null);
            blocks[blockId] = block;
            invalidateBlocksHash();
            if (concurrentMapManager.isMaster() && !concurrentMapManager.isSuperClient()) {
                block.setOwner(thisAddress);
            }
        }
        return block;
    }

    final void invalidateBlocksHash() {
        blocksHash = Integer.MIN_VALUE;
        partitionServiceImpl.reset();
    }

    final int hashBlocks() {
        if (blocksHash == Integer.MIN_VALUE) {
            int hash = 1;
            for (int i = 0; i < PARTITION_COUNT; i++) {
                Block block = blocks[i];
                hash = (hash * 31) + ((block == null) ? 0 : block.customHash());
            }
            blocksHash = hash;
        }
        return blocksHash;
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
            return b2.getOwner() == null;
        } else if (!b1.getOwner().equals(b2.getOwner())) {
            return false;
        }
        if (b1.getMigrationAddress() == null) {
            return b2.getMigrationAddress() == null;
        } else if (!b1.getMigrationAddress().equals(b2.getMigrationAddress())) {
            return false;
        }
        return true;
    }

    void fireMigrationEvent(final boolean started, Block block) {
        if (node.isMaster()) {
            if (started) {
                migrationStartTime = System.currentTimeMillis();
            } else {
                migrationStartTime = 0;
            }
        }
        final MemberImpl memberOwner = concurrentMapManager.getMember(block.getOwner());
        final MemberImpl memberMigration = concurrentMapManager.getMember(block.getMigrationAddress());
        final MigrationEvent migrationEvent = new MigrationEvent(concurrentMapManager.node, block.getBlockId(), memberOwner, memberMigration);
        partitionServiceImpl.doFireMigrationEvent(started, migrationEvent);
    }
}
