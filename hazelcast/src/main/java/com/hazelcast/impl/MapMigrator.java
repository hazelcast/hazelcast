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

import com.hazelcast.impl.concurrentmap.InitialState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.hazelcast.impl.ClusterOperation.CONCURRENT_MAP_BLOCKS;

public class MapMigrator implements Runnable {

    final static Logger logger = Logger.getLogger(MapMigrator.class.getName());

    final ConcurrentMapManager concurrentMapManager;
    final Node node;
    final int BLOCK_COUNT;
    final Block[] blocks;
    final Address thisAddress;
    final List<Block> lsBlocksToMigrate = new ArrayList<Block>(100);
    final static long MIGRATION_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(10);

    long nextMigrationMillis = System.currentTimeMillis() + MIGRATION_INTERVAL_MILLIS;

    public MapMigrator(ConcurrentMapManager concurrentMapManager) {
        this.concurrentMapManager = concurrentMapManager;
        this.node = concurrentMapManager.node;
        this.BLOCK_COUNT = ConcurrentMapManager.BLOCK_COUNT;
        this.blocks = concurrentMapManager.blocks;
        this.thisAddress = concurrentMapManager.thisAddress;
    }

    void reArrangeBlocks() {
        if (concurrentMapManager.isMaster()) {
            Map<Address, Integer> addressBlocks = getCurrentMemberBlocks();
            if (addressBlocks.size() == 0) {
                return;
            }
            List<Block> lsBlocksToRedistribute = new ArrayList<Block>();
            int aveBlockOwnCount = BLOCK_COUNT / (addressBlocks.size());
            for (Block blockReal : blocks) {
                if (blockReal.getOwner() == null) {
                    logger.log(Level.SEVERE, "Master cannot have null block owner " + blockReal);
                    return;
                }
                if (blockReal.isMigrating()) {
                    logger.log(Level.SEVERE, "Cannot have migrating block " + blockReal);
                    return;
                }
                Integer countInt = addressBlocks.get(blockReal.getOwner());
                int count = (countInt == null) ? 0 : countInt;
                if (count >= aveBlockOwnCount) {
                    lsBlocksToRedistribute.add(new Block(blockReal));
                } else {
                    addressBlocks.put(blockReal.getOwner(), ++count);
                }
            }
            Collection<Address> allAddress = addressBlocks.keySet();
            lsBlocksToMigrate.clear();
            for (Address address : allAddress) {
                Integer countInt = addressBlocks.get(address);
                int count = (countInt == null) ? 0 : countInt;
                while (count < aveBlockOwnCount && lsBlocksToRedistribute.size() > 0) {
                    Block blockToMigrate = lsBlocksToRedistribute.remove(0);
                    if (!blockToMigrate.getOwner().equals(address)) {
                        blockToMigrate.setMigrationAddress(address);
                        lsBlocksToMigrate.add(blockToMigrate);
                    }
                    count++;
                }
            }
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

    Queue qMasterMigratorTasks = new ConcurrentLinkedQueue();

    public void run() {
        if (!concurrentMapManager.isMaster()) return;
        if (concurrentMapManager.getMembers().size() < 2) return;
        long now = System.currentTimeMillis();
        if (now > nextMigrationMillis) {
            final MasterMigratorTask task = new MasterMigratorTask();
            qMasterMigratorTasks.offer(task);
            node.executorManager.executeMigrationTask(task);
            nextMigrationMillis = now + MIGRATION_INTERVAL_MILLIS;
        }
    }

    class MasterMigratorTask extends FallThroughRunnable {
        public void doRun() {
            if (qMasterMigratorTasks.poll() != null) {
                ConcurrentMapManager.MBlockMigrationCheck check = concurrentMapManager.new MBlockMigrationCheck();
                boolean isMigrating = check.call();
                if (!isMigrating) {
                    concurrentMapManager.enqueueAndReturn(new Processable() {
                        public void process() {
                            if (concurrentMapManager.isMaster()) {
                                initiateMigration();
                            }
                        }
                    });
                }
                qMasterMigratorTasks.clear();
            }
        }
    }

    void initiateMigration() {
        for (int i = 0; i < BLOCK_COUNT; i++) {
            Block block = blocks[i];
            if (block == null) {
                block = concurrentMapManager.getOrCreateBlock(i);
                block.setOwner(thisAddress);
            }
        }
        if (concurrentMapManager.getMembers().size() < 2) {
            return;
        }
        if (lsBlocksToMigrate.size() == 0) {
            reArrangeBlocks();
        }
        if (lsBlocksToMigrate.size() > 0) {
            Block block = lsBlocksToMigrate.remove(0);
            if (concurrentMapManager.isBlockInfoValid(block)) {
                if (thisAddress.equals(block.getOwner())) {
                    concurrentMapManager.doBlockInfo(block);
                } else {
                    concurrentMapManager.sendBlockInfo(block, block.getOwner());
                }
            }
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
                    block = concurrentMapManager.getOrCreateBlock(i);
                }
            }
            quickBlockRearrangement();
            Data dataAllBlocks = null;
            for (MemberImpl member : concurrentMapManager.getMembers()) {
                if (!member.localMember()) {
                    if (dataAllBlocks == null) {
                        ConcurrentMapManager.BlockOwners allBlockOwners = new ConcurrentMapManager.BlockOwners();
                        for (Block block : blocks) {
                            allBlockOwners.addBlock(block);
                        }
                        dataAllBlocks = ThreadContext.get().toData(allBlockOwners);
                    }
                    concurrentMapManager.send("blocks", CONCURRENT_MAP_BLOCKS, dataAllBlocks, member.getAddress());
                }
            }
        }
        InitialState initialState = new InitialState();
        Collection<CMap> cmaps = concurrentMapManager.maps.values();
        for (final CMap cmap : cmaps) {
            initialState.createAndAddMapState(cmap);
        }
        concurrentMapManager.sendProcessableToAll(initialState, false);
        onMembershipChange();
    }

    Map<Address, Integer> getCurrentMemberBlocks() {
        List<MemberImpl> lsMembers = concurrentMapManager.lsMembers;
        Map<Address, Integer> addressBlocks = new HashMap<Address, Integer>();
        for (MemberImpl member : lsMembers) {
            if (!member.isSuperClient()) {
                addressBlocks.put(member.getAddress(), 0);
            }
        }
        return addressBlocks;
    }

    private void quickBlockRearrangement() {
        //create all blocks
        for (int i = 0; i < BLOCK_COUNT; i++) {
            Block block = blocks[i];
            if (block == null) {
                block = concurrentMapManager.getOrCreateBlock(i);
            }
            if (block.getOwner() == null) {
                block.setOwner(thisAddress);
            }
        }
        // find storage enabled members
        Map<Address, Integer> addressBlocks = getCurrentMemberBlocks();
        if (addressBlocks.size() == 0) {
            return;
        }
        /**
         * ======= RULES =========
         * if there is no storage enabled node
         *  1. then all blocks owners are null and no migration
         * else
         *  1. all blocks are owned by a member at any given time
         *  2. empty blocks are be given (no need to migrate) to
         *  a new member immediately
         *  3. master checks if anybody is migrating before starting
         *  migration. if blockMigrating != null then it can migrate
         *  else master asks all members if they are migrating over executor
         *  thread. be careful with the fact that master can die and
         *  new master may not have blockMigrating info.
         *  4. all blocks message cannot contain migration address
         *  5. if there is at least one storage enabled member then
         *  all blocks message cannot have null block owner.
         *
         */
        // find unowned blocks
        // find owned by me but empty blocks
        // distribute them first
        // equally redistribute again
        List<Block> ownableBlocks = new ArrayList<Block>();
        for (Block blockReal : blocks) {
            if (blockReal.getOwner() == null) {
                logger.log(Level.SEVERE, "QBR cannot have block with null owner: " + blockReal);
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
        int aveBlockOwnCount = BLOCK_COUNT / (addressBlocks.size());
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
    }

    public void onMembershipChange() {
        lsBlocksToMigrate.clear();
        backupIfNextOrPreviousChanged();
        removeUnknownRecords();
    }

    private void removeUnknownRecords() {
        Collection<CMap> cmaps = concurrentMapManager.maps.values();
        for (CMap cmap : cmaps) {
            Collection<Record> records = cmap.mapRecords.values();
            for (Record record : records) {
                if (record != null) {
                    if (record.isActive()) {
                        Block block = blocks[record.getBlockId()];
                        Address owner = (block.isMigrating()) ? block.getMigrationAddress() : block.getOwner();
                        if (!thisAddress.equals(owner)) {
                            int distance = concurrentMapManager.getDistance(owner, thisAddress);
                            if (distance > cmap.getBackupCount()) {
                                cmap.markAsRemoved(record);
                            }
                        }
                    }
                }
            }
        }
    }

    public void syncForDead(Address deadAddress) {
        if (deadAddress == null || deadAddress.equals(thisAddress)) {
            return;
        }
        Set<Integer> blocksOwnedAfterDead = new HashSet<Integer>();
        for (Block block : blocks) {
            if (block != null) {
                syncForDead(deadAddress, block);
                if (thisAddress.equals(block.getOwner())) {
                    blocksOwnedAfterDead.add(block.getBlockId());
                }
            }
        }
        Collection<CMap> cmaps = concurrentMapManager.maps.values();
        for (CMap cmap : cmaps) {
            Collection<Record> records = cmap.mapRecords.values();
            for (Record record : records) {
                if (record != null) {
                    cmap.onDisconnect(record, deadAddress);
                    if (record.isActive() && blocksOwnedAfterDead.contains(record.getBlockId())) {
                        cmap.markAsOwned(record);
                        // you have to update the indexes
                        // as if this record is new so extract the values first
                        int valueHash = record.getValueHash();
                        long[] indexes = record.getIndexes();
                        byte[] indexTypes = record.getIndexTypes();
                        // set the indexes to null, (new record)
                        record.setValueHash(Integer.MIN_VALUE);
                        record.setIndexes(null, null);
                        // now update the index
                        node.queryService.updateIndex(cmap.getName(), indexes, indexTypes, record, valueHash);
                    }
                }
            }
        }
        onMembershipChange();
    }

    void syncForDead(Address deadAddress, Block block) {
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
    }

    void backupIfNextOrPreviousChanged() {
        if (node.clusterManager.isNextOrPreviousChanged()) {
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

    void migrateBlock(final Block blockInfo) {
        if (!concurrentMapManager.isBlockInfoValid(blockInfo)) {
            return;
        }
        if (!thisAddress.equals(blockInfo.getOwner())) {
            throw new RuntimeException();
        }
        if (!blockInfo.isMigrating()) {
            throw new RuntimeException();
        }
        if (blockInfo.getOwner().equals(blockInfo.getMigrationAddress())) {
            throw new RuntimeException();
        }
        Block blockReal = blocks[blockInfo.getBlockId()];
        if (blockReal.isMigrating()) {
            if (!blockInfo.getMigrationAddress().equals(blockReal.getMigrationAddress())) {
                logger.log(Level.WARNING, blockReal + ". Already migrating blockInfo is migrating again to " + blockInfo);
            } else {
                logger.log(Level.WARNING, blockInfo + " migration unknown " + blockReal);
            }
            return;
        }
        blockReal.setOwner(blockInfo.getOwner());
        blockReal.setMigrationAddress(blockInfo.getMigrationAddress());
        logger.log(Level.FINEST, "migrate blockInfo " + blockInfo);
        if (!node.isActive() || node.factory.restarted) {
            return;
        }
        if (concurrentMapManager.isSuperClient()) {
            return;
        }
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
                    if (rec.getBlockId() == blockInfo.getBlockId()) {
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
                            Block blockReal = blocks[blockInfo.getBlockId()];
                            logger.log(Level.FINEST, "migrate completing [" + blockInfo + "] realBlock " + blockReal);
                            blockReal.setOwner(blockReal.getMigrationAddress());
                            blockReal.setMigrationAddress(null);
                            logger.log(Level.FINEST, "migrate complete [" + blockInfo.getMigrationAddress() + "] now realBlock " + blockReal);
                            for (MemberImpl member : concurrentMapManager.lsMembers) {
                                if (!member.localMember()) {
                                    concurrentMapManager.sendBlockInfo(new Block(blockReal), member.getAddress());
                                }
                            }
                        }
                    });
                } catch (InterruptedException ignored) {
                }
            }
        });
    }
}
