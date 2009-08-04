/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterManager;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigProperty;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Transaction;
import com.hazelcast.impl.BlockingQueueManager.Q.ScheduledOfferAction;
import com.hazelcast.impl.BlockingQueueManager.Q.ScheduledPollAction;
import static com.hazelcast.impl.ClusterOperation.BLOCKING_QUEUE_SIZE;
import static com.hazelcast.impl.Constants.Objects.OBJECT_NULL;
import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import com.hazelcast.nio.Address;
import static com.hazelcast.nio.BufferUtil.*;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.Packet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class BlockingQueueManager extends BaseManager {
    private static final int BLOCK_SIZE = ConfigProperty.BLOCKING_QUEUE_BLOCK_SIZE.getInteger(1000);
    private final static BlockingQueueManager instance = new BlockingQueueManager();
    private final Map<String, Q> mapQueues = new HashMap<String, Q>(10);
    private final Map<Long, List<Data>> mapTxnPolledElements = new HashMap<Long, List<Data>>(10);
    private int nextIndex = 0;

    private BlockingQueueManager() {
        ClusterService.get().registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_POLL, new PacketProcessor() {
            public void process(Packet packet) {
                try {
                    handlePoll(packet);
                } catch (Throwable t) {
                    Request req = new Request();
                    req.setFromPacket(packet);
                    printState(req, false, packet.conn.getEndPoint(), 1);
                    t.printStackTrace();
                }
            }
        });
        ClusterService.get().registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_OFFER, new PacketProcessor() {
            public void process(Packet packet) {
                handleOffer(packet);
            }
        });
        ClusterService.get().registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_BACKUP_ADD, new PacketProcessor() {
            public void process(Packet packet) {
                handleBackup(packet);
            }
        });
        ClusterService.get().registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_BACKUP_REMOVE, new PacketProcessor() {
            public void process(Packet packet) {
                handleBackup(packet);
            }
        });
        ClusterService.get().registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_SIZE, new PacketProcessor() {
            public void process(Packet packet) {
                handleSize(packet);
            }
        });
        ClusterService.get().registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_PEEK, new PacketProcessor() {
            public void process(Packet packet) {
                handlePoll(packet);
            }
        });
        ClusterService.get().registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_READ, new PacketProcessor() {
            public void process(Packet packet) {
                handleRead(packet);
            }
        });
        ClusterService.get().registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_REMOVE, new PacketProcessor() {
            public void process(Packet packet) {
                handleRemove(packet);
            }
        });
        ClusterService.get().registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_TXN_BACKUP_POLL, new PacketProcessor() {
            public void process(Packet packet) {
                handleTxnBackupPoll(packet);
            }
        });
        ClusterService.get().registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_TXN_COMMIT, new PacketProcessor() {
            public void process(Packet packet) {
                handleTxnCommit(packet);
            }
        });
        ClusterService.get().registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_ADD_BLOCK, new PacketProcessor() {
            public void process(Packet packet) {
                handleAddBlock(packet);
            }
        });
        ClusterService.get().registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_REMOVE_BLOCK, new PacketProcessor() {
            public void process(Packet packet) {
                handleRemoveBlock(packet);
            }
        });
        ClusterService.get().registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_FULL_BLOCK, new PacketProcessor() {
            public void process(Packet packet) {
                handleFullBlock(packet);
            }
        });

    }

    public static BlockingQueueManager get() {
        return instance;
    }

    class BlockBackupSyncRunner implements Runnable {
        final BlockBackupSync blockSync;

        public BlockBackupSyncRunner(BlockBackupSync blockSync) {
            this.blockSync = blockSync;
        }

        public void run() {
            while (!blockSync.done) {
                try {
                    synchronized (blockSync) {
                        enqueueAndReturn(blockSync);
                        blockSync.wait();
                    }
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    class BlockBackupSync implements Processable {
        final Block block;
        final Q q;
        int index = 0;
        final int indexUpto;
        volatile boolean done = false;

        public BlockBackupSync(Q q, Block block, int indexUpto) {
            super();
            this.q = q;
            this.block = block;
            this.indexUpto = indexUpto;
        }

        public void process() {
            Data data = next();
            if (data != null && data.size() != 0) {
                q.sendBackup(true, thisAddress, data, block.blockId, index);
                index++;
                if (index > indexUpto) {
                    done = true;
                }
            } else {
                done = true;
            }
            synchronized (BlockBackupSync.this) {
                BlockBackupSync.this.notify();
            }
        }

        private Data next() {
            while (true) {
                Data data = block.get(index);
                if (data != null) {
                    return data;
                }
                index++;
                if (index > indexUpto)
                    return null;
                if (index >= BLOCK_SIZE)
                    return null;
            }
        }
    }

    public void syncForDead(Address deadAddress) {
        if (deadAddress.equals(thisAddress)) return;
        MemberImpl member = getNextMemberBeforeSync(deadAddress, true, 1);
        if (DEBUG) {
            log(deadAddress + " is dead and its backup was " + member);
        }
        Address addressNewOwner = (member == null) ? thisAddress : member.getAddress();

        Collection<Q> queues = mapQueues.values();
        for (Q q : queues) {
            List<Block> lsBlocks = q.lsBlocks;
            for (Block block : lsBlocks) {
                if (block.address.equals(deadAddress)) {
                    // set the new owner
                    block.address = addressNewOwner;
                    block.resetAddIndex();
                    if (lsMembers.size() > 1) {
                        if (addressNewOwner.equals(thisAddress)) {
                            // I am the new owner so backup to next member
                            int indexUpto = block.size() - 1;
                            if (DEBUG) {
                                log("IndexUpto " + indexUpto);
                            }
                            if (indexUpto > -1) {
                                executeLocally(new BlockBackupSyncRunner(new BlockBackupSync(q, block,
                                        indexUpto)));
                            }
                        }
                    }
                } else if (block.address.equals(thisAddress)) {
                    // If I am/was the owner of this block
                    // did my backup change..
                    // if so backup to the new next
                    if (lsMembers.size() > 1) {
                        MemberImpl memberBackupWas = getNextMemberBeforeSync(thisAddress, true, 1);
                        if (memberBackupWas == null
                                || memberBackupWas.getAddress().equals(deadAddress)) {
                            int indexUpto = block.size() - 1;
                            if (indexUpto > -1) {
                                executeLocally(new BlockBackupSyncRunner(new BlockBackupSync(q, block,
                                        indexUpto)));
                            }
                        }
                    }
                }
            }
            // packetalidate the dead member's scheduled actions
            List<ScheduledPollAction> scheduledPollActions = q.lsScheduledPollActions;
            for (ScheduledPollAction scheduledAction : scheduledPollActions) {
                if (deadAddress.equals(scheduledAction.request.caller)) {
                    scheduledAction.setValid(false);
                    ClusterManager.get().deregisterScheduledAction(scheduledAction);
                }
            }
            List<ScheduledOfferAction> scheduledOfferActions = q.lsScheduledOfferActions;
            for (ScheduledOfferAction scheduledAction : scheduledOfferActions) {
                if (deadAddress.equals(scheduledAction.request.caller)) {
                    scheduledAction.setValid(false);
                    ClusterManager.get().deregisterScheduledAction(scheduledAction);
                }
            }
        }
        doResetBlockSizes();
    }

    public void syncForAdd() {
        if (isMaster()) {
            Collection<Q> queues = mapQueues.values();
            for (Q q : queues) {
                List<Block> lsBlocks = q.lsBlocks;
                for (Block block : lsBlocks) {
                    int fullBlockId = -1;
                    if (block.isFull()) {
                        fullBlockId = block.blockId;
                    }
                    sendAddBlockMessageToOthers(block, fullBlockId, null, true);
                }
            }
        }
        Collection<Q> queues = mapQueues.values();
        for (Q q : queues) {
            List<Block> lsBlocks = q.lsBlocks;
            for (Block block : lsBlocks) {
                if (block.address.equals(thisAddress)) {
                    // If I am/was the owner of this block
                    // did my backup change..
                    // if so backup to the new next
                    if (lsMembers.size() > 1) {
                        MemberImpl memberBackupWas = getNextMemberBeforeSync(thisAddress, true, 1);
                        MemberImpl memberBackupIs = getNextMemberAfter(thisAddress, true, 1);
                        if (memberBackupWas == null || !memberBackupWas.equals(memberBackupIs)) {
                            if (DEBUG) {
                                log("Backup changed!!! so backing up to " + memberBackupIs);
                            }
                            int indexUpto = block.size() - 1;
                            if (indexUpto > -1) {
                                executeLocally(new BlockBackupSyncRunner(new BlockBackupSync(q, block,
                                        indexUpto)));
                            }
                        }
                    }
                }
            }
        }
        doResetBlockSizes();
    }

    void doResetBlockSizes() {
        Collection<Q> queues = mapQueues.values();
        for (Q q : queues) {
            // q.printStack();
            List<Block> lsBlocks = q.lsBlocks;
            int size = 0;
            for (Block block : lsBlocks) {
                if (block.address.equals(thisAddress)) {
                    size += block.size();
                }
            }
        }
    }

    final void handleSize(Packet packet) {
        Q q = getQ(packet.name);
        packet.longValue = q.size();
        sendResponse(packet);
    }

    @Override
    final void handleListenerRegisterations(boolean add, String name, Data key, Address address,
                                            boolean includeValue) {
        Q q = getQ(name);
        if (add) {
            q.addListener(address, includeValue);
        } else {
            q.removeListener(address);
        }
    }

    final void handleTxnBackupPoll(Packet packet) {
        doTxnBackupPoll(packet.txnId, doTake(packet.value));
    }

    final void handleTxnCommit(Packet packet) {
        mapTxnPolledElements.remove(packet.txnId);
    }

    final void doTxnBackupPoll(long txnId, Data value) {
        List<Data> lsTxnPolledElements = mapTxnPolledElements.get(txnId);
        if (lsTxnPolledElements == null) {
            lsTxnPolledElements = new ArrayList<Data>(1);
            mapTxnPolledElements.put(txnId, lsTxnPolledElements);
        }
        lsTxnPolledElements.add(value);
    }

    final void handleBackup(Packet packet) {
        try {
            String name = packet.name;
            int blockId = packet.blockId;
            Q q = getQ(name);
            if (packet.operation == ClusterOperation.BLOCKING_QUEUE_BACKUP_ADD) {
                Data data = doTake(packet.value);
                q.doBackup(true, data, blockId, (int) packet.longValue);
            } else if (packet.operation == ClusterOperation.BLOCKING_QUEUE_BACKUP_REMOVE) {
                q.doBackup(false, null, blockId, 0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            packet.returnToContainer();
        }
    }

    final void handleRemoveBlock(Packet packet) {
        try {
            BlockUpdate addRemove = (BlockUpdate) ThreadContext.get().toObject(packet.value);
            String name = packet.name;
            Q q = getQ(name);
            doRemoveBlock(q, packet.conn.getEndPoint(), addRemove.removeBlockId);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            packet.returnToContainer();
        }
    }

    public void doRemoveBlock(Q q, Address originalRemover, int blockId) {
        Block blockRemoved = q.removeBlock(blockId);
        if (isMaster()) {
            sendAddBlockMessageToOthers(q.name, null, blockId, -1, originalRemover, false);
        }

    }

    final void handleFullBlock(Packet packet) {
        if (isMaster()) {
            String name = packet.name;
            Q q = getQ(name);
            doFullBlock(q, packet.blockId, packet.conn.getEndPoint());
        }
        packet.returnToContainer();
    }

    final void doFullBlock(Q q, int fullBlockId, Address originalFuller) {
        int blockId = q.getLatestAddedBlock() + 1;
        Address target = nextTarget();
        Block newBlock = setFullAndCreateNewBlock(q, fullBlockId, target, blockId);
        if (newBlock != null) {
            sendAddBlockMessageToOthers(newBlock, fullBlockId, originalFuller, true);
        }
    }

    final void handleAddBlock(Packet packet) {
        try {
            BlockUpdate addRemove = (BlockUpdate) ThreadContext.get().toObject(packet.value);
            String name = packet.name;
            int blockId = addRemove.addBlockId;
            Address addressOwner = addRemove.addAddress;
            Q q = getQ(name);
            setFullAndCreateNewBlock(q, addRemove.fullBlockId, addressOwner, blockId);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            packet.returnToContainer();
        }
    }

    Block setFullAndCreateNewBlock(Q q, int fullBlockId, Address newBlockOwner, int newBlockId) {
        List<Block> lsBlocks = q.lsBlocks;
        boolean exist = false;
        for (Block block : lsBlocks) {
            if (block.blockId == newBlockId) {
                exist = true;
            }
        }
        Block newBlock = null;
        if (!exist) {
            newBlock = q.createBlock(newBlockOwner, newBlockId);
            q.addBlock(newBlock);
        }
        if (fullBlockId != -1) {
            q.setBlockFull(fullBlockId);
        }
        return newBlock;
    }

    final void handleOffer(Packet packet) {
        if (rightRemoteOfferTarget(packet)) {
            Request request = new Request();
            request.setFromPacket(packet);
            doOffer(request);
            packet.longValue = request.longValue;
            if (!request.scheduled) {
                if (request.response == Boolean.TRUE) {
                    sendResponse(packet);
                } else {
                    sendResponseFailure(packet);
                }
            } else {
                packet.returnToContainer();
            }
            request.reset();
        }
    }

    final boolean rightRemoteOfferTarget(Packet packet) {
        Q q = getQ(packet.name);
        boolean valid = q.rightPutTarget(packet.blockId);
        if (!valid) {
            sendRedoResponse(packet);
        }
        return valid;
    }

    boolean rightRemotePollTarget(Packet packet) {
        Q q = getQ(packet.name);
        boolean valid = q.rightTakeTarget(packet.blockId);
        if (!valid) {
            sendRedoResponse(packet);
        }
        return (valid);
    }

    final void handlePoll(Packet packet) {
        if (rightRemotePollTarget(packet)) {
            Request request = new Request();
            request.setFromPacket(packet);
            doPoll(request);
            if (!request.scheduled) {
                Data oldValue = (Data) request.response;
                if (oldValue != null && oldValue.size() > 0) {
                    doSet(oldValue, packet.value);
                }
                sendResponse(packet);
            } else {
                packet.returnToContainer();
            }
            request.reset();
        }
    }

    final void handleRead(Packet packet) {
        Q q = getQ(packet.name);
        q.read(packet);
    }

    final void handleRemove(Packet packet) {
        Q q = getQ(packet.name);
        q.remove(packet);
    }

    final Address nextTarget() {
        int size = lsMembers.size();
        int index = nextIndex++ % size;
        if (nextIndex >= size) {
            nextIndex = 0;
        }
        return lsMembers.get(index).getAddress();
    }

    public Q getQ(String name) {
        if (name == null)
            return null;
        Q q = mapQueues.get(name);
        if (q == null) {
            q = new Q(name);
            mapQueues.put(name, q);
        }
        return q;
    }

    public class CommitPoll extends AbstractCall {
        volatile long txnId = -1;
        String name = null;

        public ClusterOperation getOperation() {
            return ClusterOperation.BLOCKING_QUEUE_TXN_COMMIT;
        }

        public void commitPoll(String name) {
            this.name = name;
            this.txnId = ThreadContext.get().getTxnId();
            enqueueAndReturn(this);
        }

        public void handleResponse(Packet packet) {
        }

        public void process() {
            MemberImpl nextMember = getNextMemberAfter(thisAddress, true, 1);
            if (nextMember != null) {
                Address next = nextMember.getAddress();
                Packet packet = obtainPacket();
                packet.name = name;
                packet.operation = getOperation();
                packet.txnId = txnId;
                boolean sent = send(packet, next);
                if (!sent) {
                    packet.returnToContainer();
                }
            }
        }
    }

    public void destroy(String name) {
        mapQueues.remove(name);
    }

    public class QSize extends MultiCall {
        int size = 0;
        final String name;

        public int getSize() {
            int size = (Integer) call();
            TransactionImpl txn = ThreadContext.get().txn;
            if (txn != null) {
                size += txn.size(name);
            }
            return (size < 0) ? 0 : size;
        }

        public QSize(String name) {
            this.name = name;
        }

        TargetAwareOp createNewTargetAwareOp(Address target) {
            return new MGetSize(target);
        }

        boolean onResponse(Object response) {
            size += ((Long) response).intValue();
            return true;
        }

        void onCall() {
            size = 0;
        }

        Object returnResult() {
            return size;
        }

        class MGetSize extends MigrationAwareTargettedCall {
            public MGetSize(Address target) {
                this.target = target;
                request.reset();
                request.name = name;
                request.operation = BLOCKING_QUEUE_SIZE;
            }

            @Override
            void handleNoneRedoResponse(final Packet packet) {
                handleLongNoneRedoResponse(packet);
            }

            @Override
            public void doLocalOp() {
                Q q = getQ(name);
                request.response = (long) q.size();
                setResult(request.response);
            }
        }
    }

    class QIterator<E> implements Iterator<E>, Processable {

        volatile String name;
        volatile List<Integer> blocks = null;
        int currentBlockId = -1;
        int currentIndex = -1;
        Object next = null;
        boolean hasNextCalled = false;
        Iterator txnOffers = null;

        public void set(String name) {
            TransactionImpl txn = ThreadContext.get().txn;
            if (txn != null) {
                List txnOfferItems = txn.newEntries(name);
                if (txnOfferItems != null) {
                    txnOffers = txnOfferItems.iterator();
                }
            }
            synchronized (QIterator.this) {
                this.name = name;
                enqueueAndReturn(QIterator.this);
                try {
                    QIterator.this.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public void process() {
            Q q = getQ(name);
            List<Block> lsBlocks = q.lsBlocks;
            blocks = new ArrayList<Integer>(lsBlocks.size());
            for (Block block : lsBlocks) {
                blocks.add(block.blockId);
            }
            synchronized (QIterator.this) {
                QIterator.this.notify();
            }
        }

        public boolean hasNext() {
            next = null;
            if (txnOffers != null) {
                boolean hasInTxn = txnOffers.hasNext();
                hasNextCalled = true;
                if (hasInTxn) {
                    next = txnOffers.next();
                    return true;
                } else {
                    txnOffers = null;
                }
            }
            while (next == null) {
                boolean canRead = setNextBlock();
                // System.out.println(currentBlockId + " Can read " + canRead);
                if (!canRead)
                    return false;
                Read read = new Read();
                next = read.read(name, currentBlockId, currentIndex);
                // System.out.println(currentIndex + " Next " + next);
                if (next == null) {
                    currentIndex = -1;
                } else {
                    currentIndex = read.readIndex;
                    currentIndex++;
                }
            }
            hasNextCalled = true;
            return true;
        }

        boolean setNextBlock() {
            if (currentIndex == -1 || currentIndex >= BLOCK_SIZE) {
                if (blocks.size() == 0) {
                    return false;
                } else {
                    currentBlockId = blocks.remove(0);
                    currentIndex = 0;
                    return true;
                }
            }
            return true;
        }

        public E next() {
            if (!hasNextCalled) {
                boolean hasNext = hasNext();
                if (!hasNext) {
                    return null;
                }
            }
            if (next != null) {
                hasNextCalled = false;
            }
            return (E) next;
        }

        public void remove() {
        }

    }

    public class Remove extends QueueBasedCall {
        String name;
        int blockId;
        int index;

        public Object remove(String name, int blockId, int index) {
            this.name = name;
            this.blockId = blockId;
            this.index = index;
            enqueueAndReturn(this);
            Object result;
            try {
                result = responses.take();
                if (result == OBJECT_NULL)
                    return null;
                else {
                    return ThreadContext.get().toObject((Data) result);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        void setResponse() {
            responses.add(OBJECT_NULL);
        }

        public void handleResponse(Packet packet) {
            setResponse();
            packet.returnToContainer();
        }

        @Override
        public void onDisconnect(Address dead) {
            process();
        }

        public void process() {
            addCall(this);
            Q q = getQ(name);

            Address target = q.getBlockOwner(blockId);
            if (target == null) {
                responses.add(OBJECT_NULL);
            } else if (target.equals(thisAddress)) {
                q.remove(this);
            } else {
                Packet packet = obtainPacket();
                packet.name = name;
                packet.operation = getOperation();
                packet.callId = getId();
                packet.blockId = blockId;
                packet.timeout = 0;
                packet.longValue = index;
                boolean sent = false;
                if (target != null) {
                    sent = send(packet, target);
                }
                if (target == null || !sent) {
                    packet.returnToContainer();
                    responses.add(OBJECT_NULL);
                }
            }
        }

        public ClusterOperation getOperation() {
            return ClusterOperation.BLOCKING_QUEUE_REMOVE;
        }

    }

    public class Read extends QueueBasedCall {
        String name;
        int blockId;
        int index;
        int readIndex = 0;

        public Object read(String name, int blockId, int index) {
            this.name = name;
            this.blockId = blockId;
            this.index = index;
            enqueueAndReturn(this);
            Object result;
            try {
                result = responses.take();
                if (result == OBJECT_NULL)
                    return null;
                else {
                    return ThreadContext.get().toObject((Data) result);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        void setResponse(Data value, int index) {
            responses.clear();
            readIndex = index;
            responses.add((value == null) ? OBJECT_NULL : value);
        }

        public void handleResponse(Packet packet) {
            Data value = doTake(packet.value);
            int indexRead = (int) packet.longValue;
            setResponse(value, indexRead);
            removeCall(getId());
            packet.returnToContainer();
        }

        @Override
        public void onDisconnect(Address dead) {
            removeCall(getId());
            enqueueAndReturn(Read.this);
        }

        public void process() {
            responses.clear();
            Q q = getQ(name);
            Address target = q.getBlockOwner(blockId);
            if (target == null) {
                responses.add(OBJECT_NULL);
            } else if (target.equals(thisAddress)) {
                q.read(this);
            } else {
                addCall(this);
                Packet packet = obtainPacket();
                packet.name = name;
                packet.operation = getOperation();
                packet.callId = getId();
                packet.blockId = blockId;
                packet.timeout = 0;
                packet.longValue = index;
                boolean sent = send(packet, target);
                if (!sent) {
                    packet.returnToContainer();
                    onDisconnect(target);
                }
            }
        }

        public ClusterOperation getOperation() {
            return ClusterOperation.BLOCKING_QUEUE_READ;
        }

    }

    class Offer extends BooleanOp {

        int doCount = 1;

        public boolean offer(String name, Object value, long timeout) {
            return offer(name, value, timeout, true);
        }

        public boolean offer(String name, Object value, long timeout, boolean transactional) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.txn;
            if (transactional && txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                txn.attachPutOp(name, null, value, true);
            } else {
                return booleanCall(ClusterOperation.BLOCKING_QUEUE_OFFER, name, null, value, timeout, -1);
            }
            return true;
        }

        @Override
        void handleNoneRedoResponse(Packet packet) {
            if (request.operation == ClusterOperation.BLOCKING_QUEUE_OFFER
                    && packet.responseType == Constants.ResponseTypes.RESPONSE_SUCCESS) {
                if (!zeroBackup) {
                    if (getPreviousMemberBefore(thisAddress, true, 1).getAddress().equals(
                            packet.conn.getEndPoint())) {
                        int itemIndex = (int) packet.longValue;
                        if (itemIndex != -1) {
                            Q q = getQ(request.name);
                            if (request.value == null || request.value.size() == 0) {
                                throw new RuntimeException("Invalid data " + request.value);
                            }
                            q.doBackup(true, request.value, request.blockId, (int) packet.longValue);
                        }
                    }
                }
            }
            super.handleNoneRedoResponse(packet);
        }

        @Override
        public void process() {
            if (doCount++ % 4 == 0) {
                printState(request, true, target, doCount);
            }
            super.process();
        }

        @Override
        public void setTarget() {
            target = getTargetForOffer(request);
        }

        @Override
        public void doLocalOp() {
            Q q = getQ(request.name);
            if (q.rightPutTarget(request.blockId)) {
                doOffer(request);
                if (!request.scheduled) {
                    setResult(request.response);
                }
            } else {
                setResult(OBJECT_REDO);
            }
        }
    }

    public Address getTargetForOffer(Request request) {
        Address target = null;
        Q q = getQ(request.name);
        Block block = q.getCurrentPutBlock();
        if (block == null) {
            target = getMasterAddress();
            request.blockId = 0;
        } else {
            target = block.address;
            request.blockId = block.blockId;
        }
        return target;
    }

    class Poll extends TargetAwareOp {

        public Object peek(String name) {
            return objectCall(ClusterOperation.BLOCKING_QUEUE_PEEK, name, null, null, 0, -1);
        }

        public Object poll(String name, long timeout) {
            Object value = objectCall(ClusterOperation.BLOCKING_QUEUE_POLL, name, null, null, timeout, -1);
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.txn;
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                txn.attachRemoveOp(name, null, value, false);
            }
            return value;
        }

        @Override
        public void setTarget() {
            Q q = getQ(request.name);
            Block takeBlock = q.getCurrentTakeBlock();
            if (takeBlock == null) {
                target = getMasterAddress();
                request.blockId = 0;
            } else {
                target = takeBlock.address;
                request.blockId = takeBlock.blockId;
            }
        }

        @Override
        public void doLocalOp() {
            Q q = getQ(request.name);
            if (q.rightTakeTarget(request.blockId)) {
                doPoll(request);
                if (!request.scheduled) {
                    setResult(request.response);
                }
            } else {
                setResult(OBJECT_REDO);
            }
        }

        @Override
        void handleNoneRedoResponse(Packet packet) {
            if (request.operation == ClusterOperation.BLOCKING_QUEUE_POLL
                    && packet.responseType == Constants.ResponseTypes.RESPONSE_SUCCESS) {
                if (!zeroBackup) {
                    if (getPreviousMemberBefore(thisAddress, true, 1).getAddress().equals(
                            packet.conn.getEndPoint())) {
                        if (packet.value != null) {
                            Q q = getQ(packet.name);
                            q.doBackup(false, null, request.blockId, 0);
                        }
                    }
                }
            }
            super.handleNoneRedoResponse(packet);
        }
    }

    void printState(Request request, boolean local, Address address, int doCount) {
        Q q = getQ(request.name);
        q.printStack();
        System.out.println(local + " ===== CurrentRequest === " + doCount);
        System.out.println("blockId: " + request.blockId);
        System.out.println("operation: " + request.operation);
        System.out.println("version: " + request.version);
        System.out.println("timeout: " + request.timeout);
        System.out.println("target: " + address);
        System.out.println("=========== DONE =======");
    }

    void doOffer(Request req) {
        Q q = getQ(req.name);
        if (q.blCurrentPut == null) {
            q.setCurrentPut();
        }
        if (q.quickSize() >= q.maxSizePerJVM) {
            if (req.hasEnoughTimeToSchedule()) {
                req.scheduled = true;
                final Request reqScheduled = (req.local) ? req : req.hardCopy();
                if (reqScheduled.local) {
                    if (reqScheduled.attachment == null) {
                        throw new RuntimeException("Scheduled local but attachement is null");
                    }
                }
                q.scheduleOffer(reqScheduled);
            } else {
                req.response = Boolean.FALSE;
            }
            return;
        }
        q.offer(req);
        req.value = null;
        req.response = Boolean.TRUE;
    }

    void doPoll(final Request req) {
        Q q = getQ(req.name);
        if (q.blCurrentTake == null) {
            q.setCurrentTake();
        }
        if (!q.blCurrentTake.containsValidItem() && !q.blCurrentTake.isFull()) {
            if (req.hasEnoughTimeToSchedule()) {
                req.scheduled = true;
                final Request reqScheduled = (req.local) ? req : req.hardCopy();
                if (reqScheduled.local) {
                    if (reqScheduled.attachment == null) {
                        throw new RuntimeException("Scheduled local but attachement is null");
                    }
                }
                q.schedulePoll(reqScheduled);
            } else {
                req.response = null;
            }
            return;
        }
        Data value = null;
        if (req.operation == ClusterOperation.BLOCKING_QUEUE_PEEK) {
            value = q.peek();
        } else {
            value = q.poll(req);
        }
        req.response = value;
    }

    public void sendAddBlockMessageToOthers(Block block, int fullBlockId, Address except,
                                            boolean add) {
        sendAddBlockMessageToOthers(block.name, block.address, block.blockId, fullBlockId, except, add);
    }

    public void sendAddBlockMessageToOthers(String name, Address addAddress, int blockId, int fullBlockId, Address except,
                                            boolean add) {
        ClusterOperation operation = ClusterOperation.BLOCKING_QUEUE_ADD_BLOCK;
        if (!add) {
            operation = ClusterOperation.BLOCKING_QUEUE_REMOVE_BLOCK;
        }
        if (lsMembers.size() > 1) {
            int addBlockId = -1;
            int removeBlockId = -1;
            if (add) {
                addBlockId = blockId;
                except = null;
            } else {
                removeBlockId = blockId;
            }
            BlockUpdate addRemove = new BlockUpdate(addAddress, addBlockId, fullBlockId,
                    removeBlockId);
            for (MemberImpl member : lsMembers) {
                if (!member.localMember()) {
                    Address dest = member.getAddress();
                    if (!dest.equals(except)) {
                        send(name, operation, addRemove, dest);
                    }
                }
            }
        }
    }

    public void sendFullMessage(Block block) {
        try {
            Packet packet = ThreadContext.get().getPacketPool().obtain();
            packet.set(block.name, ClusterOperation.BLOCKING_QUEUE_FULL_BLOCK, null, null);
            packet.blockId = block.blockId;
            Address master = getMasterAddress();
            boolean sent = send(packet, master);
            if (!sent)
                packet.returnToContainer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public class Q {
        String name;
        List<Block> lsBlocks = new ArrayList<Block>(10);
        Block blCurrentPut = null;
        Block blCurrentTake = null;
        int latestAddedBlock = -1;

        List<ScheduledPollAction> lsScheduledPollActions = new ArrayList<ScheduledPollAction>(100);
        List<ScheduledOfferAction> lsScheduledOfferActions = new ArrayList<ScheduledOfferAction>(
                100);
        final int maxSizePerJVM;

        Map<Address, Boolean> mapListeners = new HashMap<Address, Boolean>(2);

        final long maxAge;

        public Q(String name) {
            QueueConfig qconfig = Config.get().getQueueConfig(name.substring(2));
            maxSizePerJVM = (qconfig.getMaxSizePerJVM() == 0) ? QueueConfig.DEFAULT_MAX_SIZE_PER_JVM : qconfig.getMaxSizePerJVM();
            maxAge = (qconfig.getTimeToLiveSeconds() == 0) ? QueueConfig.DEFAULT_TTL_SECONDS : qconfig.getTimeToLiveSeconds() * 1000l;
            log(name + ".maxSizePerJVM=" + maxSizePerJVM);
            log(name + ".maxAge=" + maxAge);

            this.name = name;
            Address master = getMasterAddress();
            if (master.isThisAddress()) {
                Block block = createBlock(master, 0);
                addBlock(block);
                sendAddBlockMessageToOthers(block, -1, null, true);
                for (int i = 0; i < 9; i++) {
                    int blockId = i + 1;
                    Address target = nextTarget();
                    block = createBlock(target, blockId);
                    addBlock(block);
                    sendAddBlockMessageToOthers(block, -1, null, true);
                }
            }
        }

        public Block createBlock(Address address, int blockId) {
            return new Block(address, blockId, maxAge, name);
        }

        public Address getBlockOwner(int blockId) {
            for (Block block : lsBlocks) {
                if (block.blockId == blockId)
                    return block.address;
            }
            return null;
        }

        public void addListener(Address address, boolean includeValue) {
            mapListeners.put(address, includeValue);
        }

        public void removeListener(Address address) {
            mapListeners.remove(address);
        }

        public void scheduleOffer(Request request) {
            ScheduledOfferAction action = new ScheduledOfferAction(request);
            lsScheduledOfferActions.add(action);
            ClusterManager.get().registerScheduledAction(action);

        }

        public void schedulePoll(Request request) {
            ScheduledPollAction action = new ScheduledPollAction(request);
            lsScheduledPollActions.add(action);
            ClusterManager.get().registerScheduledAction(action);
        }

        public class ScheduledPollAction extends ScheduledAction {

            public ScheduledPollAction(Request request) {
                super(request);
            }

            @Override
            public boolean consume() {
                Q q = getQ(request.name);
                valid = rightTakeTarget(request.blockId);
                if (valid) {
                    Data value = poll(request);
                    request.response = value;
                    request.key = null;
                    request.value = null;
                    returnScheduledAsSuccess(request);
                    return true;
                } else {
                    onExpire();
                    return false;
                }
            }

            @Override
            public void onExpire() {
                request.response = null;
                request.key = null;
                request.value = null;
                returnScheduledAsSuccess(request);
            }
        }

        public class ScheduledOfferAction extends ScheduledAction {

            public ScheduledOfferAction(Request request) {
                super(request);
            }

            @Override
            public boolean consume() {
                // didn't expire but is it valid?
                Q q = getQ(request.name);
                valid = q.rightPutTarget(request.blockId);
                if (valid) {
                    offer(request);
                    request.response = Boolean.TRUE;
                    request.key = null;
                    request.value = null;
                    returnScheduledAsBoolean(request);
                    return true;
                } else {
                    onExpire();
                    return false;
                }
            }

            @Override
            public void onExpire() {
                request.response = Boolean.FALSE;
                returnScheduledAsBoolean(request);
            }
        }

        public Block getBlock(int blockId) {
            int size = lsBlocks.size();
            for (int i = 0; i < size; i++) {
                Block block = lsBlocks.get(i);
                if (block.blockId == blockId) {
                    return block;
                }
            }
            return null;
        }

        public void setBlockFull(int fullBlockId) {
            // System.out.println("FULL block " + fullBlockId);
            if (blCurrentPut != null && blCurrentPut.blockId == fullBlockId) {
                blCurrentPut.setFull(true);
                blCurrentPut = null;
            }
            if (blCurrentTake != null && blCurrentTake.blockId == fullBlockId) {
                blCurrentTake.setFull(true);
            }
            int size = lsBlocks.size();
            for (int i = 0; i < size; i++) {
                Block block = lsBlocks.get(i);
                if (block.blockId == fullBlockId) {
                    block.setFull(true);
                    blCurrentPut = null;
                    return;
                }
            }
        }

        public Block removeBlock(int blockId) {
            try {
                if (blCurrentPut != null && blCurrentPut.blockId == blockId) {
                    blCurrentPut = null;
                }

                if (blCurrentTake != null && blCurrentTake.blockId == blockId) {
                    blCurrentTake = null;
                }
                int size = lsBlocks.size();
                for (int i = 0; i < size; i++) {
                    Block b = lsBlocks.get(i);
                    if (b.blockId == blockId) {
                        blCurrentTake = null;
                        return lsBlocks.remove(i);
                    }
                }
                return null;
            } finally {
                if (blCurrentPut == null) {
                    setCurrentPut();
                }
            }
        }

        public void addBlock(Block newBlock) {
            lsBlocks.add(newBlock);
            latestAddedBlock = Math.max(newBlock.blockId, latestAddedBlock);
            // System.out.println("Adding newBlock " + newBlock);
            // printStack();
        }

        int getLatestAddedBlock() {
            return latestAddedBlock;
        }

        void remove(Remove remove) {
            Block block = getBlock(remove.blockId);
            if (block != null) {
                block.remove(remove.index);
            }
            remove.setResponse();
        }

        void remove(Packet packet) {
            int index = (int) packet.longValue;
            int blockId = packet.blockId;
            Block block = getBlock(blockId);
            packet.longValue = -1;
            if (block != null) {
                block.remove(index);
            }
            sendResponse(packet);
        }

        void read(Read read) {
            Block block = getBlock(read.blockId);
            if (block == null) {
                read.setResponse(null, -1);
                return;
            }
            for (int i = read.index; i < BLOCK_SIZE; i++) {
                Data data = block.get(i);
                if (data != null) {
                    Data value = ThreadContext.get().hardCopy(data);
                    read.setResponse(value, i);
                    return;
                }
            }
            read.setResponse(null, -1);
        }

        void read(Packet packet) {
            int index = (int) packet.longValue;
            int blockId = packet.blockId;
            Block block = getBlock(blockId);
            packet.longValue = -1;
            if (block != null) {
                blockLoop:
                for (int i = index; i < BLOCK_SIZE; i++) {
                    Data data = block.get(i);
                    if (data != null) {
                        doHardCopy(data, packet.value);
                        packet.longValue = i;
                        break blockLoop;
                    }
                }
            }
            sendResponse(packet);
        }

        void doFireEntryEvent(boolean add, Data value) {
            if (mapListeners.size() == 0)
                return;
            if (add) {
                fireMapEvent(mapListeners, name, EntryEvent.TYPE_ADDED, value);
            } else {
                fireMapEvent(mapListeners, name, EntryEvent.TYPE_REMOVED, value);
            }
        }

        long getRecordId(int blockId, int addIndex) {
            return (blockId * BLOCK_SIZE) + addIndex;
        }

        int offer(Request req) {
            try {
                int addIndex = blCurrentPut.add(req.value);
                long recordId = getRecordId(blCurrentPut.blockId, addIndex);
                req.recordId = recordId;
                doFireEntryEvent(true, req.value);
                sendBackup(true, req.caller, req.value, blCurrentPut.blockId, addIndex);
                req.longValue = addIndex;
                if (blCurrentPut.isFull()) {
                    fireBlockFullEvent(blCurrentPut);
                    blCurrentPut = null;
                    setCurrentPut();
                }
                return addIndex;
            } finally {
                boolean consumed = false;
                while (!consumed && lsScheduledPollActions.size() > 0) {
                    ScheduledAction pollAction = lsScheduledPollActions.remove(0);
                    ClusterManager.get().deregisterScheduledAction(pollAction);
                    if (!pollAction.expired()) {
                        consumed = pollAction.consume();
                    }
                }
            }
        }

        public Data poll(Request request) {
            try {
                setCurrentTake();
                Data value = blCurrentTake.remove();
                if (request.txnId != -1) {
                    MemberImpl backup = null;
                    if (request.caller.equals(thisAddress)) {
                        backup = getNextMemberAfter(thisAddress, true, 1);
                    } else {
                        backup = getNextMemberAfter(request.caller, true, 1);
                    }
                    if (backup != null) {
                        if (backup.getAddress().equals(thisAddress)) {
                            doTxnBackupPoll(request.txnId, value);
                        } else {
                            sendTxnBackup(backup.getAddress(), value, request.txnId);
                        }
                    }
                }
                request.recordId = getRecordId(blCurrentTake.blockId, blCurrentTake.removeIndex);
                doFireEntryEvent(false, value);

                sendBackup(false, request.caller, null, blCurrentTake.blockId, 0);
                if (!blCurrentTake.containsValidItem() && blCurrentTake.isFull()) {
                    fireBlockRemoveEvent(blCurrentTake);
                    blCurrentTake = null;
                }
                return value;
            } finally {
                boolean consumed = false;
                while (!consumed && lsScheduledOfferActions.size() > 0) {
                    ScheduledOfferAction offerAction = lsScheduledOfferActions.remove(0);
                    ClusterManager.get().deregisterScheduledAction(offerAction);
                    if (!offerAction.expired()) {
                        consumed = offerAction.consume();
                    }
                }
            }
        }

        public Data peek() {
            setCurrentTake();
            if (blCurrentTake == null)
                return null;
            Data value = blCurrentTake.peek();
            if (value == null) {
                return null;
            }
            return doHardCopy(value);
        }

        boolean doBackup(boolean add, Data data, int blockId, int addIndex) {
            if (zeroBackup) return true;
            Block block = getBlock(blockId);
            if (block == null)
                return false;
            if (block.address.equals(thisAddress)) {
                return false;
            }
            if (add) {
                boolean added = block.add(addIndex, data);
                return true;
            } else {
                if (block.size() > 0) {
                    block.remove();
                } else {
                    return false;
                }
            }
            return true;
        }

        boolean sendTxnBackup(Address address, Data value, long txnId) {
            if (zeroBackup) return true;
            Packet packet = obtainPacket(name, null, value, ClusterOperation.BLOCKING_QUEUE_TXN_BACKUP_POLL, 0);
            packet.txnId = txnId;
            boolean sent = send(packet, address);
            if (!sent) {
                packet.returnToContainer();
            }
            return sent;
        }

        boolean sendBackup(boolean add, Address packetoker, Data data, int blockId, int addIndex) {
            if (zeroBackup)
                return true;
            if (addIndex == -1)
                throw new RuntimeException("addIndex cannot be -1");
            if (lsMembers.size() > 1) {
                if (getNextMemberAfter(thisAddress, true, 1).getAddress().equals(packetoker)) {
                    return true;
                }
                ClusterOperation operation = ClusterOperation.BLOCKING_QUEUE_BACKUP_REMOVE;
                if (add) {
                    operation = ClusterOperation.BLOCKING_QUEUE_BACKUP_ADD;
                    data = doHardCopy(data);
                }
                Packet packet = obtainPacket(name, null, data, operation, 0);
                packet.blockId = blockId;
                packet.longValue = addIndex;
                boolean sent = send(packet, getNextMemberAfter(thisAddress, true, 1).getAddress());
                if (!sent) {
                    packet.returnToContainer();
                }
                return sent;
            } else {
                return true;
            }
        }

        void fireBlockRemoveEvent(Block block) {
            if (isMaster()) {
                doRemoveBlock(Q.this, null, block.blockId);
            } else {
                removeBlock(block.blockId);
                sendAddBlockMessageToOthers(block, -1, null, false);
            }
        }

        void fireBlockFullEvent(Block block) {
            if (isMaster()) {
                doFullBlock(Q.this, block.blockId, null);
            } else {
                sendFullMessage(block);
            }
        }

        int quickSize() {
            int size = 0;
            int length = lsBlocks.size();
            for (int i = 0; i < length; i++) {
                Block block = lsBlocks.get(i);
                if (thisAddress.equals(block.address)) {
                    size += block.quickSize();
                }
            }
            return size;
        }

        int size() {
            int size = 0;
            int length = lsBlocks.size();
            for (int i = 0; i < length; i++) {
                Block block = lsBlocks.get(i);
                if (thisAddress.equals(block.address)) {
                    size += block.size();
                }
            }
            return size;
        }

        boolean rightPutTarget(int blockId) {
            setCurrentPut();
            return (blCurrentPut != null)
                    && (blCurrentPut.blockId == blockId)
                    && (thisAddress.equals(blCurrentPut.address))
                    && (!blCurrentPut.isFull());
        }

        boolean rightTakeTarget(int blockId) {
            setCurrentTake();
            return (blCurrentTake != null)
                    && (blCurrentTake.blockId == blockId)
                    && (thisAddress.equals(blCurrentTake.address))
                    && (blCurrentTake.containsValidItem() || !blCurrentTake.isFull());
        }

        void setCurrentPut() {
            if (blCurrentPut == null || blCurrentPut.isFull()) {
                int size = lsBlocks.size();
                for (int i = 0; i < size; i++) {
                    Block block = lsBlocks.get(i);
                    if (!block.isFull()) {
                        blCurrentPut = block;
                        return;
                    }
                }
            }
        }

        Block getCurrentPutBlock() {
            setCurrentPut();
            return blCurrentPut;
        }

        Block getCurrentTakeBlock() {
            setCurrentTake();
            return blCurrentTake;
        }

        public void setCurrentTake() {
            if (blCurrentTake == null || !blCurrentTake.containsValidItem()) {
                if (lsBlocks.size() > 0) {
                    blCurrentTake = lsBlocks.get(0);
                }
            }
        }

        @Override
        public String toString() {
            return "Q{name='" + name + "'}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Q q = (Q) o;

            if (name != null ? !name.equals(q.name) : q.name != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        void printStack() {
            System.out.println("=========================");
            System.out.println(Hazelcast.getCluster());
            System.out.println("== " + thisAddress + " ==");
            for (Block block : lsBlocks) {
                System.out.println(block);
            }
            System.out.println("--------------------");
            System.out.println("CurrenTake " + blCurrentTake);
            System.out.println("CurrentPut " + blCurrentPut);
            System.out.println("== " + new Date() + " ==");
        }
    }

    class Block {
        final int blockId;
        final String name;
        final Data[] values;
        final long maxAge;
        Address address;
        int addIndex = 0;
        int removeIndex = 0;
        boolean full = false;

        public Block(Address address, int blockId, long maxAge, String name) {
            super();
            this.address = address;
            this.blockId = blockId;
            this.maxAge = maxAge;
            this.name = name;
            this.values = new Data[BLOCK_SIZE];
        }

        public Data peek() {
            for (int i = removeIndex; i < BLOCK_SIZE; i++) {
                if (values[i] != null) {
                    Data value = values[i];
                    removeIndex = i;
                    return value;
                }
            }
            return null;
        }

        public Data get(int index) {
            return values[index];
        }

        void resetAddIndex() {
            int index = BLOCK_SIZE - 1;
            while (index >= 0) {
                if (values[index] != null) {
                    addIndex = index + 1;
                    if (addIndex >= BLOCK_SIZE) {
                        full = true;
                    }
                    return;
                }
                index--;
            }
            index = 0;
        }

        public int add(Data data) {
            data.createDate = System.currentTimeMillis();
            if (values != null) {
                if (values[addIndex] != null)
                    return -1;
                values[addIndex] = data;
            }
            int addedCurrentIndex = addIndex;
            addIndex++;
            if (addIndex >= BLOCK_SIZE) {
                full = true;
            }
            return addedCurrentIndex;
        }

        public boolean add(int index, Data data) {
            data.createDate = System.currentTimeMillis();
            if (values[index] != null)
                return false;
            values[index] = data;
            return true;
        }

        boolean isFull() {
            return full;
        }

        public void setFull(boolean full) {
            this.full = full;
        }

        public Data remove(int index) {
            Data value = values[index];
            values[index] = null;
            return value;
        }

        public Data remove() {
            for (int i = removeIndex; i < addIndex; i++) {
                if (values[i] != null) {
                    Data value = values[i];
                    values[i] = null;
                    removeIndex = i + 1;
                    return value;
                }
            }
            return null;
        }

        public boolean containsValidItem() {
            for (int i = removeIndex; i < addIndex; i++) {
                if (values[i] != null) {
                    Data value = values[i];
                    long age = System.currentTimeMillis() - value.createDate;
                    if (age > maxAge) {
                        values[i] = null;
                    } else {
                        return true;
                    }
                }
            }
            return false;
        }

        public int size() {
            int s = 0;
            for (int i = removeIndex; i < addIndex; i++) {
                if (values[i] != null) {
                    Data value = values[i];
                    long age = System.currentTimeMillis() - value.createDate;
                    if (age > maxAge) {
                        values[i] = null;
                    } else {
                        s++;
                    }
                }
            }
            return s;
        }

        public int quickSize() {
            return (addIndex - removeIndex);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Block block = (Block) o;

            if (blockId != block.blockId) return false;
            if (name != null ? !name.equals(block.name) : block.name != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = blockId;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Block [" + blockId + "] full=" + isFull() + ", size=" + size() + ", " + address;
        }
    }

    public static class BlockUpdate implements DataSerializable {
        int addBlockId = -1;
        int removeBlockId = -1;
        int fullBlockId = -1;
        Address addAddress = null;

        public BlockUpdate(Address addAddress, int addBlockId, int fullBlockId, int removeBlockId) {
            this.addAddress = addAddress;
            this.addBlockId = addBlockId;
            this.fullBlockId = fullBlockId;
            this.removeBlockId = removeBlockId;
        }

        public BlockUpdate() {
        }

        public void readData(DataInput in) throws IOException {
            addBlockId = in.readInt();
            removeBlockId = in.readInt();
            fullBlockId = in.readInt();
            if (addBlockId != -1) {
                addAddress = new Address();
                addAddress.readData(in);
            }
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeInt(addBlockId);
            out.writeInt(removeBlockId);
            out.writeInt(fullBlockId);
            if (addBlockId != -1) {
                addAddress.writeData(out);
            }
        }
    }
}
