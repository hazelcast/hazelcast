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

import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Prefix;
import com.hazelcast.core.Transaction;
import com.hazelcast.impl.BlockingQueueManager.Q.ScheduledOfferAction;
import com.hazelcast.impl.BlockingQueueManager.Q.ScheduledPollAction;
import com.hazelcast.impl.base.PacketProcessor;
import com.hazelcast.impl.base.RuntimeInterruptedException;
import com.hazelcast.impl.base.ScheduledAction;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.Packet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.impl.ClusterOperation.BLOCKING_QUEUE_SIZE;
import static com.hazelcast.impl.Constants.Objects.OBJECT_NULL;
import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import static com.hazelcast.nio.IOUtil.toObject;

public class BlockingQueueManager extends BaseManager {
    private final int BLOCK_SIZE;
    private final Map<String, Q> mapQueues = new ConcurrentHashMap<String, Q>(10);
    private final Map<Long, List<Data>> mapTxnPolledElements = new HashMap<Long, List<Data>>(10);
    private int nextIndex = 0;

    BlockingQueueManager(Node node) {
        super(node);
        BLOCK_SIZE = node.groupProperties.BLOCKING_QUEUE_BLOCK_SIZE.getInteger();
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_POLL, new PacketProcessor() {
            public void process(Packet packet) {
                try {
                    handlePoll(packet);
                } catch (Throwable t) {
                    Request req = Request.copy(packet);
                    printState(req, false, packet.conn.getEndPoint(), 1);
                    t.printStackTrace();
                }
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_OFFER, new PacketProcessor() {
            public void process(Packet packet) {
                handleOffer(packet);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_OFFER_FIRST, new PacketProcessor() {
            public void process(Packet packet) {
                handleOfferFirst(packet);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_BACKUP_ADD, new PacketProcessor() {
            public void process(Packet packet) {
                handleBackup(packet);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_BACKUP_REMOVE, new PacketProcessor() {
            public void process(Packet packet) {
                handleBackup(packet);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_SIZE, new PacketProcessor() {
            public void process(Packet packet) {
                handleSize(packet);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_PEEK, new PacketProcessor() {
            public void process(Packet packet) {
                handlePoll(packet);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_READ, new PacketProcessor() {
            public void process(Packet packet) {
                handleRead(packet);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_REMOVE, new PacketProcessor() {
            public void process(Packet packet) {
                handleRemove(packet);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_TXN_BACKUP_POLL, new PacketProcessor() {
            public void process(Packet packet) {
                handleTxnBackupPoll(packet);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_TXN_COMMIT, new PacketProcessor() {
            public void process(Packet packet) {
                handleTxnCommit(packet);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_ADD_BLOCK, new PacketProcessor() {
            public void process(Packet packet) {
                handleAddBlock(packet);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_REMOVE_BLOCK, new PacketProcessor() {
            public void process(Packet packet) {
                handleRemoveBlock(packet);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_QUEUE_FULL_BLOCK, new PacketProcessor() {
            public void process(Packet packet) {
                handleFullBlock(packet);
            }
        });
    }

    public void collectMemberStats(MemberStateImpl memberStats) {
        Collection<Q> queues = mapQueues.values();
        for (Q queue : queues) {
            memberStats.putLocalQueueStats(queue.getName(), queue.getQueueStats());
        }
    }

    // todo: this part can be implemented better.
    // Thread.sleep is bad!
    class BlockBackupSyncRunner extends FallThroughRunnable {
        final BlockBackupSync blockSync;

        public BlockBackupSyncRunner(BlockBackupSync blockSync) {
            this.blockSync = blockSync;
        }

        public void doRun() {
            while (!blockSync.done) {
                try {
                    synchronized (blockSync) {
                        enqueueAndReturn(blockSync);
                        blockSync.wait();
                    }
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    class BlockBackupSync implements Processable {
        final Block block;
        final Q q;
        int index = 0;
        volatile boolean done = false;

        public BlockBackupSync(Q q, Block block) {
            super();
            this.q = q;
            this.block = block;
        }

        public void process() {
            Data data = next();
            if (data != null && data.size() != 0) {
                q.sendBackup(true, thisAddress, data, block.blockId, index);
            } else {
                done = true;
            }
            synchronized (BlockBackupSync.this) {
                BlockBackupSync.this.notify();
            }
        }

        private Data next() {
            while (index < BLOCK_SIZE) {
                QData data = block.get(index++);
                if (data != null) {
                    return data.data;
                }
            }
            return null;
        }
    }

    public void syncForDead(Address deadAddress) {
        if (deadAddress.equals(thisAddress)) return;
        MemberImpl member = getNextMemberBeforeSync(deadAddress, true, 1);
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
                            int size = block.size();
                            if (size > 0) {
                                executeLocally(new BlockBackupSyncRunner(new BlockBackupSync(q, block)));
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
                            int size = block.size();
                            if (size > 0) {
                                executeLocally(new BlockBackupSyncRunner(new BlockBackupSync(q, block)));
                            }
                        }
                    }
                }
            }
            // validate the dead member's scheduled actions
            List<ScheduledPollAction> scheduledPollActions = q.lsScheduledPollActions;
            for (ScheduledPollAction scheduledAction : scheduledPollActions) {
                if (deadAddress.equals(scheduledAction.getRequest().caller)) {
                    scheduledAction.setValid(false);
                    node.clusterManager.deregisterScheduledAction(scheduledAction);
                }
            }
            List<ScheduledOfferAction> scheduledOfferActions = q.lsScheduledOfferActions;
            for (ScheduledOfferAction scheduledAction : scheduledOfferActions) {
                if (deadAddress.equals(scheduledAction.getRequest().caller)) {
                    scheduledAction.setValid(false);
                    node.clusterManager.deregisterScheduledAction(scheduledAction);
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
                            int size = block.size();
                            if (size > 0) {
                                executeLocally(new BlockBackupSyncRunner(new BlockBackupSync(q, block)));
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
    final void handleListenerRegistrations(boolean add, String name, Data key, Address address,
                                           boolean includeValue) {
        Q q = getQ(name);
        if (add) {
            q.addListener(address, includeValue);
        } else {
            q.removeListener(address);
        }
    }

    final void handleTxnBackupPoll(Packet packet) {
        doTxnBackupPoll(packet.txnId, packet.getValueData());
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
                Data data = packet.getValueData();
                q.doBackup(true, data, blockId, (int) packet.longValue);
            } else if (packet.operation == ClusterOperation.BLOCKING_QUEUE_BACKUP_REMOVE) {
                q.doBackup(false, null, blockId, (int) packet.longValue);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            releasePacket(packet);
        }
    }

    final void handleRemoveBlock(Packet packet) {
        try {
            BlockUpdate addRemove = (BlockUpdate) ThreadContext.get().toObject(packet.getValueData());
            String name = packet.name;
            Q q = getQ(name);
            doRemoveBlock(q, packet.conn.getEndPoint(), addRemove.removeBlockId);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            releasePacket(packet);
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
        releasePacket(packet);
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
            BlockUpdate addRemove = (BlockUpdate) ThreadContext.get().toObject(packet.getValueData());
            String name = packet.name;
            int blockId = addRemove.addBlockId;
            Address addressOwner = addRemove.addAddress;
            Q q = getQ(name);
            setFullAndCreateNewBlock(q, addRemove.fullBlockId, addressOwner, blockId);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            releasePacket(packet);
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
            Request request = Request.copy(packet);
            doOffer(request);
            packet.longValue = request.longValue;
            if (!request.scheduled) {
                if (request.response == Boolean.TRUE) {
                    sendResponse(packet);
                } else {
                    sendResponseFailure(packet);
                }
            } else {
                releasePacket(packet);
            }
        }
    }

    final void handleOfferFirst(Packet packet) {
        if (rightRemotePollTarget(packet)) {
            Request request = Request.copy(packet);
            doOfferFirst(request);
            packet.longValue = request.longValue;
            if (!request.scheduled) {
                if (request.response == Boolean.TRUE) {
                    sendResponse(packet);
                } else {
                    sendResponseFailure(packet);
                }
            } else {
                releasePacket(packet);
            }
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
            boolean sent = sendRedoResponse(packet);
        }
        return (valid);
    }

    final void handlePoll(Packet packet) {
        if (rightRemotePollTarget(packet)) {
            Request request = Request.copy(packet);
            doPoll(request);
            packet.longValue = request.longValue;
            if (!request.scheduled) {
                Data oldValue = (Data) request.response;
                if (oldValue != null && oldValue.size() > 0) {
                    packet.setValue(oldValue);
                }
                sendResponse(packet);
            } else {
                releasePacket(packet);
            }
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
            TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
            this.txnId = txn.getId();
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
                    releasePacket(packet);
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
            TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
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

        class MGetSize extends MigrationAwareTargetedCall {
            public MGetSize(Address target) {
                this.target = target;
                request.reset();
                request.name = name;
                request.operation = BLOCKING_QUEUE_SIZE;
                request.setLongRequest();
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
        int nextBlockId = -1;
        int nextIndex = -1;
        boolean hasNextCalled = false;
        Iterator txnOffers = null;

        public void set(String name) {
            TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
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
                    nextBlockId = currentBlockId;
                    nextIndex = currentIndex;
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
            if (nextBlockId != -1 && nextIndex != -1) {
                Remove remove = new Remove();
                remove.remove(name, nextBlockId, nextIndex);
            }
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
                    return toObject((Data) result);
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
            releasePacket(packet);
        }

        @Override
        public void onDisconnect(Address dead) {
            process();
        }

        public void process() {
            Q q = getQ(name);
            Address target = q.getBlockOwner(blockId);
            if (target == null) {
                responses.add(OBJECT_NULL);
            } else if (target.equals(thisAddress)) {
                q.remove(this);
            } else {
                addCall(this);
                Packet packet = obtainPacket();
                packet.name = name;
                packet.operation = getOperation();
                packet.callId = getCallId();
                packet.blockId = blockId;
                packet.timeout = 0;
                packet.longValue = index;
                boolean sent = false;
                if (target != null) {
                    sent = send(packet, target);
                }
                if (target == null || !sent) {
                    releasePacket(packet);
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
            Data value = packet.getValueData();
            int indexRead = (int) packet.longValue;
            setResponse(value, indexRead);
            removeCall(getCallId());
            releasePacket(packet);
        }

        @Override
        public void onDisconnect(Address dead) {
            removeCall(getCallId());
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
                packet.callId = getCallId();
                packet.blockId = blockId;
                packet.timeout = 0;
                packet.longValue = index;
                boolean sent = send(packet, target);
                if (!sent) {
                    releasePacket(packet);
                    onDisconnect(target);
                }
            }
        }

        public ClusterOperation getOperation() {
            return ClusterOperation.BLOCKING_QUEUE_READ;
        }
    }

    class Offer extends TargetAwareOp {

        public boolean offer(String name, Object value, long timeout) throws InterruptedException {
            return offer(ClusterOperation.BLOCKING_QUEUE_OFFER, name, value, timeout, true);
        }

        public boolean offer(String name, Object value, long timeout, boolean transactional) throws InterruptedException {
            return offer(ClusterOperation.BLOCKING_QUEUE_OFFER, name, value, timeout, transactional);
        }

        protected boolean offer(ClusterOperation operation, String name, Object value, long timeout, boolean transactional) throws InterruptedException {
            try {
                ThreadContext threadContext = ThreadContext.get();
                TransactionImpl txn = threadContext.getCallContext().getTransaction();
                if (transactional && txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                    txn.attachPutOp(name, null, value, timeout, true);
                } else {
                    return booleanCall(operation, name, null, value, timeout, -1);
                }
                return true;
            } catch (RuntimeInterruptedException e) {
                throw new InterruptedException();
            }
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

    class OfferFirst extends Offer {
        public boolean offer(String name, Object value, long timeout) throws InterruptedException {
            return offer(name, value, timeout, true);
        }

        public boolean offer(String name, Object value, long timeout, boolean transactional) throws InterruptedException {
            return offer(ClusterOperation.BLOCKING_QUEUE_OFFER_FIRST, name, value, timeout, transactional);
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
                doOfferFirst(request);
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

        public Object poll(String name, long timeout) throws InterruptedException {
            try {
                Object value = objectCall(ClusterOperation.BLOCKING_QUEUE_POLL, name, null, null, timeout, -1);
                ThreadContext threadContext = ThreadContext.get();
                TransactionImpl txn = threadContext.getCallContext().getTransaction();
                if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                    txn.attachRemoveOp(name, null, value, false);
                }
                return value;
            } catch (RuntimeInterruptedException rie) {
                throw new InterruptedException();
            }
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
        if (req.value == null) {
            throw new RuntimeException("Offer request value cannot be null. Local:" + req.local);
        }
        if (req.value.size() == 0) {
            throw new RuntimeException("Offer request value size cannot be zero. Local:" + req.local);
        }
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
                        throw new RuntimeException("Scheduled local but attachment is null");
                    }
                }
                q.scheduleOffer(reqScheduled);
            } else {
                req.response = Boolean.FALSE;
            }
            return;
        }
        q.offer(req, false);
        req.response = Boolean.TRUE;
    }

    void doOfferFirst(Request req) {
        if (req.value == null) {
            throw new RuntimeException("OfferFirst request value cannot be null. Local:" + req.local);
        }
        if (req.value.size() == 0) {
            throw new RuntimeException("OfferFirst request value size cannot be zero. Local:" + req.local);
        }
        Q q = getQ(req.name);
        if (q.blCurrentTake == null) {
            q.setCurrentTake();
        }
        if (q.quickSize() >= q.maxSizePerJVM || q.blCurrentTake.hasNoSpace()) {
            if (req.hasEnoughTimeToSchedule()) {
                req.scheduled = true;
                final Request reqScheduled = (req.local) ? req : req.hardCopy();
                if (reqScheduled.local) {
                    if (reqScheduled.attachment == null) {
                        throw new RuntimeException("Scheduled local but attachment is null");
                    }
                }
                q.scheduleOfferFirst(reqScheduled);
            } else {
                req.response = Boolean.FALSE;
            }
            return;
        }
        q.offer(req, true);
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
                if (req.local) {
                    if (req.attachment == null) {
                        throw new RuntimeException("Scheduled local but attachment is null");
                    }
                }
                q.schedulePoll(req);
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

    public void sendFullMessageToMaster(Block block) {
        Packet packet = obtainPacket();
        packet.set(block.name, ClusterOperation.BLOCKING_QUEUE_FULL_BLOCK, null, null);
        packet.blockId = block.blockId;
        Address master = getMasterAddress();
        boolean sent = send(packet, master);
        if (!sent)
            releasePacket(packet);
    }

    public void appendState(StringBuffer sb) {
        Collection<Q> queues = mapQueues.values();
        for (Q queue : queues) {
            queue.appendState(sb);
        }
    }

    public final class Q {
        String name;
        final List<Block> lsBlocks;
        Block blCurrentPut = null;
        Block blCurrentTake = null;
        int latestAddedBlock = -1;

        final List<ScheduledPollAction> lsScheduledPollActions = new ArrayList<ScheduledPollAction>(100);
        final List<ScheduledOfferAction> lsScheduledOfferActions = new ArrayList<ScheduledOfferAction>(100);
        final int maxSizePerJVM;

        Map<Address, Boolean> mapListeners = new HashMap<Address, Boolean>(2);

        final long maxAge;

        public Q(String name) {
            QueueConfig qconfig = node.getConfig().getQueueConfig(name.substring(Prefix.QUEUE.length()));
            maxSizePerJVM = (qconfig.getMaxSizePerJVM() == 0) ? Integer.MAX_VALUE : qconfig.getMaxSizePerJVM();
            maxAge = (qconfig.getTimeToLiveSeconds() == 0) ? Integer.MAX_VALUE :
                    TimeUnit.SECONDS.toMillis(qconfig.getTimeToLiveSeconds());
            logger.log(Level.FINEST, name + ".maxSizePerJVM=" + maxSizePerJVM);
            logger.log(Level.FINEST, name + ".maxAge=" + maxAge);
            this.name = name;
            final int blocksLimit = 10;
            lsBlocks = new ArrayList<Block>(blocksLimit);
            Address master = getMasterAddress();
            if (master == null) {
                throw new IllegalStateException("Master is " + master);
            }
            if (master.isThisAddress()) {
                Block block = createBlock(master, 0);
                addBlock(block);
                sendAddBlockMessageToOthers(block, -1, null, true);
                for (int i = 1; i < blocksLimit; i++) {
                    int blockId = i;
                    Address target = nextTarget();
                    block = createBlock(target, blockId);
                    addBlock(block);
                    sendAddBlockMessageToOthers(block, -1, null, true);
                }
            }
        }

        public LocalQueueStatsImpl getQueueStats() {
            long now = System.currentTimeMillis();
            int ownedCount = 0;
            int backupCount = 0;
            long minAge = Long.MAX_VALUE;
            long maxAge = Long.MIN_VALUE;
            long totalAge = 0;
            for (Block block : lsBlocks) {
                if (thisAddress.equals(block.address)) {
                    QData[] values = block.getValues();
                    for (QData value : values) {
                        if (value != null) {
                            ownedCount++;
                            long age = (now - value.createDate);
                            minAge = Math.min(minAge, age);
                            maxAge = Math.max(maxAge, age);
                            totalAge += age;
                        }
                    }
                } else {
                    backupCount += block.size();
                }
            }
            long aveAge = (ownedCount == 0) ? 0 : (totalAge / ownedCount);
            return new LocalQueueStatsImpl(ownedCount, backupCount, minAge, maxAge, aveAge);
        }

        public void appendState(StringBuffer sb) {
            sb.append("\nQ.name: ").append(name).append(" this:").append(thisAddress);
            sb.append("\n\tlatestAdded:").append(latestAddedBlock);
            sb.append(" put:").append(blCurrentPut);
            sb.append(" take:").append(blCurrentTake);
            sb.append(" s.polls:").append(lsScheduledPollActions.size());
            sb.append(", s.offers:").append(lsScheduledOfferActions.size());
            sb.append(", lsBlocks:").append(lsBlocks.size());
            for (Block block : lsBlocks) {
                sb.append("\n\t").append(block.blockId).append(":").
                        append(block.size()).append(" ").append(block.address);
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
            node.clusterManager.registerScheduledAction(action);
        }

        public void scheduleOfferFirst(Request request) {
            ScheduledOfferFirstAction action = new ScheduledOfferFirstAction(request);
            lsScheduledOfferActions.add(action);
            node.clusterManager.registerScheduledAction(action);
        }

        public void schedulePoll(Request request) {
            ScheduledPollAction action = new ScheduledPollAction(request);
            lsScheduledPollActions.add(action);
            node.clusterManager.registerScheduledAction(action);
        }

        public int getMaxSizePerJVM() {
            return maxSizePerJVM;
        }

        public String getName() {
            return name;
        }

        public class ScheduledPollAction extends ScheduledAction {

            public ScheduledPollAction(Request request) {
                super(request);
            }

            @Override
            public boolean consume() {
                valid = rightTakeTarget(request.blockId);
                if (valid) {
                    request.response = poll(request);
                    returnScheduledAsSuccess(request);
                    return true;
                } else {
                    request.response = OBJECT_REDO;
                    returnResponse(request);
                    return false;
                }
            }

            @Override
            public void onExpire() {
                request.response = null;
                returnScheduledAsSuccess(request);
                lsScheduledPollActions.remove(ScheduledPollAction.this);
            }
        }

        public class ScheduledOfferFirstAction extends ScheduledOfferAction {

            public ScheduledOfferFirstAction(Request request) {
                super(request);
            }

            @Override
            public boolean consume() {
                valid = rightTakeTarget(request.blockId);
                if (valid) {
                    offer(request, true);
                    request.response = Boolean.TRUE;
                    returnScheduledAsBoolean(request);
                    return true;
                } else {
                    request.response = OBJECT_REDO;
                    returnResponse(request);
                    return false;
                }
            }
        }

        public class ScheduledOfferAction extends ScheduledAction {

            public ScheduledOfferAction(Request request) {
                super(request);
            }

            @Override
            public boolean consume() {
                // didn't expire but is it valid?
                valid = rightPutTarget(request.blockId);
                if (valid) {
                    offer(request, false);
                    request.response = Boolean.TRUE;
                    returnScheduledAsBoolean(request);
                    return true;
                } else {
                    request.response = OBJECT_REDO;
                    returnResponse(request);
                    return false;
                }
            }

            @Override
            public void onExpire() {
                request.response = Boolean.FALSE;
                returnScheduledAsBoolean(request);
                lsScheduledOfferActions.remove(ScheduledOfferAction.this);
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
                QData data = block.get(i);
                if (data != null) {
                    read.setResponse(data.data, i);
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
                for (int i = index; i < BLOCK_SIZE; i++) {
                    QData data = block.get(i);
                    if (data != null) {
                        packet.setValue(data.data);
                        packet.longValue = i;
                        break;
                    }
                }
            }
            sendResponse(packet);
        }

        void doFireEntryEvent(boolean add, Data value, Address callerAddress) {
            if (mapListeners.size() == 0)
                return;
            if (add) {
                fireMapEvent(mapListeners, name, EntryEvent.TYPE_ADDED, value, callerAddress);
            } else {
                fireMapEvent(mapListeners, name, EntryEvent.TYPE_REMOVED, value, callerAddress);
            }
        }

        int offer(Request req, boolean first) {
            if (req.value == null) {
                throw new RuntimeException("Offer request value cannot be null. Local:" + req.local);
            }
            if (req.value.size() == 0) {
                throw new RuntimeException("Offer request value size cannot be zero. Local:" + req.local);
            }
            try {
                int addIndex = (first) ? blCurrentTake.addFirst(req.value) : blCurrentPut.add(req.value);
                doFireEntryEvent(true, req.value, req.caller);
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
                    if (!pollAction.expired()) {
                        consumed = pollAction.consume();
                        node.clusterManager.deregisterScheduledAction(pollAction);
                    }
                }
            }
        }

        public Data poll(Request request) {
            try {
                setCurrentTake();
                QData qdata = blCurrentTake.remove();
                int removeIndex = blCurrentTake.removeIndex - 1;
                request.longValue = removeIndex;
                Data value = (qdata == null) ? null : qdata.data;
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
                doFireEntryEvent(false, value, request.caller);
                sendBackup(false, request.caller, null, blCurrentTake.blockId, removeIndex);
                if (!blCurrentTake.containsValidItem() && blCurrentTake.isFull()) {
                    fireBlockRemoveEvent(blCurrentTake);
                    blCurrentTake = null;
                }
                return value;
            } catch (Throwable t) {
                t.printStackTrace();
                return null;
            } finally {
                boolean consumed = false;
                while (!consumed && lsScheduledOfferActions.size() > 0) {
                    ScheduledOfferAction offerAction = lsScheduledOfferActions.remove(0);
                    if (!offerAction.expired()) {
                        consumed = offerAction.consume();
                        node.clusterManager.deregisterScheduledAction(offerAction);
                    }
                }
            }
        }

        public Data peek() {
            setCurrentTake();
            if (blCurrentTake == null)
                return null;
            QData value = blCurrentTake.peek();
            if (value == null) {
                return null;
            }
            return value.data;
        }

        boolean doBackup(boolean add, Data data, int blockId, int itemIndex) {
            if (zeroBackup) return true;
            Block block = getBlock(blockId);
            if (block == null)
                return false;
            if (thisAddress.equals(block.address)) {
                return false;
            }
            if (add) {
                boolean added = block.add(itemIndex, data);
                return true;
            } else {
                if (block.size() > 0) {
                    block.remove(itemIndex);
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
                releasePacket(packet);
            }
            return sent;
        }

        boolean sendBackup(boolean add, Address caller, Data data, int blockId, int addIndex) {
            if (zeroBackup)
                return true;
            if (addIndex == -1)
                throw new RuntimeException("addIndex cannot be -1");
            if (lsMembers.size() > 1) {
                MemberImpl memberBackup = getNextMemberAfter(thisAddress, true, 1);
                if (memberBackup == null) {
                    return true;
                }
                ClusterOperation operation = ClusterOperation.BLOCKING_QUEUE_BACKUP_REMOVE;
                if (add) {
                    operation = ClusterOperation.BLOCKING_QUEUE_BACKUP_ADD;
                }
                Packet packet = obtainPacket(name, null, data, operation, 0);
                packet.blockId = blockId;
                packet.longValue = addIndex;
                boolean sent = send(packet, memberBackup.getAddress());
                if (!sent) {
                    releasePacket(packet);
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
                setBlockFull(block.blockId);
                sendFullMessageToMaster(block);
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
                    if (block != null && !block.isFull()) {
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
            if (!(o instanceof Q)) return false;
            Q q = (Q) o;
            return name == null ? q.name == null : name.equals(q.name);
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
            System.out.println("CurrentTake " + blCurrentTake);
            System.out.println("CurrentPut " + blCurrentPut);
            System.out.println("== " + new Date() + " ==");
        }
    }

    class QData {
        final Data data;
        final long createDate;

        QData(Data data) {
            this.data = data;
            this.createDate = System.currentTimeMillis();
        }
    }

    class Block {
        final int blockId;
        final String name;
        final QData[] values;
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
            this.values = new QData[BLOCK_SIZE];
        }

        public QData peek() {
            for (int i = removeIndex; i < BLOCK_SIZE; i++) {
                if (values[i] != null) {
                    QData value = values[i];
                    removeIndex = i;
                    return value;
                }
            }
            return null;
        }

        public QData[] getValues() {
            return values;
        }

        public QData get(int index) {
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

        public int addFirst(Data item) {
            final QData data = new QData(item);
            List<QData> ordered = new ArrayList<QData>(BLOCK_SIZE);
            for (QData d : values) {
                if (d != null) {
                    ordered.add(d);
                }
            }
            values[0] = data;
            int i = 1;
            for (QData d : ordered) {
                values[i++] = d;
            }
            addIndex = i;
            removeIndex = 0;
            return 0;
        }

        public int add(Data item) {
            QData data = new QData(item);
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

        public boolean add(int index, Data item) {
            QData data = new QData(item);
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

        public QData remove(int index) {
            QData value = values[index];
            values[index] = null;
            return value;
        }

        public QData remove() {
            for (int i = removeIndex; i < addIndex; i++) {
                if (values[i] != null) {
                    QData value = values[i];
                    values[i] = null;
                    removeIndex = i + 1;
                    return value;
                }
            }
            return null;
        }

        public boolean hasNoSpace() {
            for (int i = 0; i < BLOCK_SIZE; i++) {
                if (values[i] == null) {
                    return false;
                }
            }
            return true;
        }

        public boolean containsValidItem() {
            for (int i = removeIndex; i < addIndex; i++) {
                if (values[i] != null) {
                    QData value = values[i];
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
            boolean owner = (thisAddress.equals(address));
            int start = (owner) ? removeIndex : 0;
            int end = (owner) ? addIndex : BLOCK_SIZE;
            for (int i = start; i < end; i++) {
                if (values[i] != null) {
                    QData value = values[i];
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
