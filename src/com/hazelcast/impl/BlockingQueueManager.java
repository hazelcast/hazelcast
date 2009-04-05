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

import static com.hazelcast.impl.Constants.BlockingQueueOperations.OP_B_ADD_BLOCK;
import static com.hazelcast.impl.Constants.BlockingQueueOperations.OP_B_ADD_TOPIC_LISTENER;
import static com.hazelcast.impl.Constants.BlockingQueueOperations.OP_B_BACKUP_ADD;
import static com.hazelcast.impl.Constants.BlockingQueueOperations.OP_B_BACKUP_REMOVE;
import static com.hazelcast.impl.Constants.BlockingQueueOperations.OP_B_FULL_BLOCK;
import static com.hazelcast.impl.Constants.BlockingQueueOperations.OP_B_OFFER;
import static com.hazelcast.impl.Constants.BlockingQueueOperations.OP_B_PEEK;
import static com.hazelcast.impl.Constants.BlockingQueueOperations.OP_B_POLL;
import static com.hazelcast.impl.Constants.BlockingQueueOperations.OP_B_PUBLISH;
import static com.hazelcast.impl.Constants.BlockingQueueOperations.OP_B_READ;
import static com.hazelcast.impl.Constants.BlockingQueueOperations.OP_B_REMOVE;
import static com.hazelcast.impl.Constants.BlockingQueueOperations.OP_B_REMOVE_BLOCK;
import static com.hazelcast.impl.Constants.BlockingQueueOperations.OP_B_SIZE;
import static com.hazelcast.impl.Constants.BlockingQueueOperations.OP_B_TXN_BACKUP_POLL;
import static com.hazelcast.impl.Constants.BlockingQueueOperations.OP_B_TXN_COMMIT;
import static com.hazelcast.impl.Constants.Objects.OBJECT_NULL;
import static com.hazelcast.nio.BufferUtil.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.impl.BlockingQueueManager.Q.ScheduledOfferAction;
import com.hazelcast.impl.BlockingQueueManager.Q.ScheduledPollAction;
import com.hazelcast.impl.ClusterManager.AbstractRemotelyProcessable;
import com.hazelcast.impl.Config.QConfig;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.PacketQueue;
import com.hazelcast.nio.PacketQueue.Packet;

class BlockingQueueManager extends BaseManager {
	private final Request remoteReq = new Request();
	private final static BlockingQueueManager instance = new BlockingQueueManager();
	private final Map<String, Q> mapQueues = new HashMap<String, Q>(10);
	private final Map<Long, List<Data>> mapTxnPolledElements = new HashMap<Long, List<Data>>(10);
	private static final int BLOCK_SIZE = 1000;
	private int nextIndex = 0;

	private BlockingQueueManager() {
		ClusterService.get().registerPacketProcessor(OP_B_POLL, new PacketProcessor() {
			public void process(PacketQueue.Packet packet) {
				handlePoll(packet);
			}
		});
		ClusterService.get().registerPacketProcessor(OP_B_OFFER, new PacketProcessor() {
			public void process(PacketQueue.Packet packet) {
				handleOffer(packet);
			}
		});
		ClusterService.get().registerPacketProcessor(OP_B_BACKUP_ADD, new PacketProcessor() {
			public void process(PacketQueue.Packet packet) {
				handleBackup(packet);
			}
		});
		ClusterService.get().registerPacketProcessor(OP_B_BACKUP_REMOVE, new PacketProcessor() {
			public void process(Packet packet) {
				handleBackup(packet);
			}
		});
		ClusterService.get().registerPacketProcessor(OP_B_PUBLISH, new PacketProcessor() {
			public void process(PacketQueue.Packet packet) {
				handlePublish(packet);
			}
		});
		ClusterService.get().registerPacketProcessor(OP_B_ADD_TOPIC_LISTENER, new PacketProcessor() {
			public void process(PacketQueue.Packet packet) {
				handleAddTopicListener(packet);
			}
		});
		ClusterService.get().registerPacketProcessor(OP_B_SIZE, new PacketProcessor() {
			public void process(PacketQueue.Packet packet) {
				handleSize(packet);
			}
		});
		ClusterService.get().registerPacketProcessor(OP_B_PEEK, new PacketProcessor() {
			public void process(PacketQueue.Packet packet) {
				handlePoll(packet);
			}
		});
		ClusterService.get().registerPacketProcessor(OP_B_READ, new PacketProcessor() {
			public void process(PacketQueue.Packet packet) {
				handleRead(packet);
			}
		});
		ClusterService.get().registerPacketProcessor(OP_B_REMOVE, new PacketProcessor() {
			public void process(PacketQueue.Packet packet) {
				handleRemove(packet);
			}
		});
		ClusterService.get().registerPacketProcessor(OP_B_TXN_BACKUP_POLL, new PacketProcessor() {
			public void process(PacketQueue.Packet packet) {
				handleTxnBackupPoll(packet);
			}
		});
		ClusterService.get().registerPacketProcessor(OP_B_TXN_COMMIT, new PacketProcessor() {
			public void process(PacketQueue.Packet packet) {
				handleTxnCommit(packet);
			}
		});
		ClusterService.get().registerPacketProcessor(OP_B_ADD_BLOCK, new PacketProcessor() {
			public void process(PacketQueue.Packet packet) {
				handleAddBlock(packet);
			}
		});
		ClusterService.get().registerPacketProcessor(OP_B_REMOVE_BLOCK, new PacketProcessor() {
			public void process(PacketQueue.Packet packet) {
				handleRemoveBlock(packet);
			}
		});
		ClusterService.get().registerPacketProcessor(OP_B_FULL_BLOCK, new PacketProcessor() {
			public void process(PacketQueue.Packet packet) {
				handleFullBlock(packet);
			}
		});
		
	}

	public static BlockingQueueManager get() {
		return instance;
	}

	class BlockBackupSyncRunner implements Runnable {
		BlockBackupSync blockSync = null;

		public BlockBackupSyncRunner(BlockBackupSync blockSync) {
			super();
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
			if (data != null) {
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

	void syncForDead(Address addressDead) {
		MemberImpl member = getNextMemberBeforeSync(addressDead, true, 1);
		if (DEBUG) {
			log(addressDead + " is dead and its backup was " + member);
		}
		Address addressNewOwner = (member == null) ? thisAddress : member.getAddress();

		Collection<Q> queues = mapQueues.values();
		for (Q q : queues) {
			List<Block> lsBlocks = q.lsBlocks;
			for (Block block : lsBlocks) {
				if (block.address.equals(addressDead)) {
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
								new Thread(new BlockBackupSyncRunner(new BlockBackupSync(q, block,
										indexUpto))).start();
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
								|| memberBackupWas.getAddress().equals(addressDead)) {
							int indexUpto = block.size() - 1;
							if (indexUpto > -1) {
								new Thread(new BlockBackupSyncRunner(new BlockBackupSync(q, block,
										indexUpto))).start();
							}
						}
					}
				}
			}
			// packetalidate the dead member's scheduled actions
			List<ScheduledPollAction> scheduledPollActions = q.lsScheduledPollActions;
			for (ScheduledPollAction scheduledAction : scheduledPollActions) {
				if (addressDead.equals(scheduledAction.request.caller)) {
					scheduledAction.setValid(false);
					ClusterManager.get().deregisterScheduledAction(scheduledAction);
				}
			}
			List<ScheduledOfferAction> scheduledOfferActions = q.lsScheduledOfferActions;
			for (ScheduledOfferAction scheduledAction : scheduledOfferActions) {
				if (addressDead.equals(scheduledAction.request.caller)) {
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
								new Thread(new BlockBackupSyncRunner(new BlockBackupSync(q, block,
										indexUpto))).start();
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
			q.size = size;
		}
	}

	final void handleSize(PacketQueue.Packet packet) {
		Q q = getQ(packet.name);
		if (DEBUG) {
			q.printStack();
		}
		packet.longValue = q.size;
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

	final void handleTxnBackupPoll(PacketQueue.Packet packet) {
		doTxnBackupPoll(packet.txnId, doTake(packet.value));
	}

	final void handleTxnCommit(PacketQueue.Packet packet) {
		List<Data> lsTxnPolledElements = mapTxnPolledElements.remove(packet.txnId);
	}

	final void doTxnBackupPoll(long txnId, Data value) {
		List<Data> lsTxnPolledElements = mapTxnPolledElements.get(txnId);
		if (lsTxnPolledElements == null) {
			lsTxnPolledElements = new ArrayList<Data>(1);
			mapTxnPolledElements.put(txnId, lsTxnPolledElements);
		}
		lsTxnPolledElements.add(value);
	}

	final void handleBackup(PacketQueue.Packet packet) {
		try {
			String name = packet.name;
			int blockId = packet.blockId;
			Q q = getQ(name);
			if (packet.operation == OP_B_BACKUP_ADD) {
				Data data = doTake(packet.value);
				q.doBackup(true, data, blockId, (int) packet.longValue);
			} else if (packet.operation == OP_B_BACKUP_REMOVE) {
				q.doBackup(false, null, blockId, 0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			packet.returnToContainer();
		}
	}

	final void handleAddBlock(PacketQueue.Packet packet) {
		try {
			BlockUpdate addRemove = (BlockUpdate) ThreadContext.get().toObject(packet.value);
			String name = packet.name;
			int blockId = addRemove.addBlockId;
			Address addressOwner = addRemove.addAddress;
			Q q = getQ(name);
			List<Block> lsBlocks = q.lsBlocks;
			boolean exist = false;
			for (Block block : lsBlocks) {
				if (block.blockId == blockId) {
					exist = true;
				}
			}
			if (!exist) {
				Block newBlock = q.createBlock(addressOwner, blockId);
				q.addBlock(newBlock);
			}
			if (addRemove.fullBlockId != -1) {
				q.setBlockFull(addRemove.fullBlockId);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			packet.returnToContainer();
		}
	}

	final void handleRemoveBlock(PacketQueue.Packet packet) {
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
		if (blockRemoved != null) {
			if (isMaster()) {
				sendAddBlockMessageToOthers(blockRemoved, -1, originalRemover, false);
			}
		}
	}

	final void handleFullBlock(PacketQueue.Packet packet) {
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
		Block newBlock = q.createBlock(target, blockId);
		q.addBlock(newBlock);
		if (fullBlockId != -1) {
			q.setBlockFull(fullBlockId);
		}
		sendAddBlockMessageToOthers(newBlock, fullBlockId, originalFuller, true);
	}

	final void handleOffer(PacketQueue.Packet packet) {
		if (rightRemoteOfferTarget(packet)) {
			remoteReq.setPacket(packet);
			doOffer(remoteReq);
			packet.longValue = remoteReq.longValue;
			if (!remoteReq.scheduled) {
				if (remoteReq.response == Boolean.TRUE) {
					sendResponse(packet);
				} else {
					sendResponseFailure(packet);
				}
			} else {
				packet.returnToContainer();
			}
			remoteReq.reset();
		}
	}

	final void handlePublish(Packet packet) {
		if (rightRemoteOfferTarget(packet)) {
			remoteReq.setPacket(packet);
			doPublish(remoteReq);
			packet.longValue = remoteReq.longValue;
			if (!remoteReq.scheduled) {
				if (remoteReq.response == Boolean.TRUE) {
					sendResponse(packet);
				} else {
					sendResponseFailure(packet);
				}
			} else {
				packet.returnToContainer();
			}
			remoteReq.reset();
		}
	}

	final void handleAddTopicListener(PacketQueue.Packet packet) {
		if (rightRemoteOfferTarget(packet)) {
			remoteReq.setPacket(packet);
			doAddTopicListener(remoteReq);
			packet.longValue = remoteReq.longValue;
			sendResponse(packet);
			remoteReq.reset();
		}
	}

	final boolean rightRemoteOfferTarget(PacketQueue.Packet packet) {
		Q q = getQ(packet.name);
		if (q.blCurrentPut == null) {
			q.setCurrentPut();
		}
		boolean packetalid = false;
		if (packet.blockId != q.blCurrentPut.blockId) {
			if (packet.blockId > q.blCurrentPut.blockId) {
				int size = q.lsBlocks.size();
				for (int i = 0; i < size; i++) {
					Block block = q.lsBlocks.get(i);
					if (block.blockId == packet.blockId) {
						if (thisAddress.equals(block.address)) {
							q.blCurrentPut = block;
						}
					}
				}
			} else {
				packetalid = true;
			}
		}
		if (q.blCurrentPut.isFull() || !thisAddress.equals(q.blCurrentPut.address)) {
			packetalid = true;
		}
		if (packetalid) {
			sendRedoResponse(packet);
		}
		return (!packetalid);
	}

	boolean rightRemotePollTarget(Packet packet) {
		Q q = getQ(packet.name);
		if (q.blCurrentTake == null) {
			q.setCurrentTake();
		}
		boolean packetalid = false;
		if (packet.blockId != q.blCurrentTake.blockId) {
			if (packet.blockId > q.blCurrentTake.blockId) {
				int size = q.lsBlocks.size();
				for (int i = 0; i < size; i++) {
					Block block = q.lsBlocks.get(i);
					if (block.blockId == packet.blockId) {
						if (thisAddress.equals(block.address)) {
							q.blCurrentTake = block;
						}
					}
				}
			} else {
				packetalid = true;
			}
		}
		if ((q.blCurrentTake.size() == 0 && q.blCurrentTake.isFull())
				|| !thisAddress.equals(q.blCurrentTake.address)) {
			packetalid = true;
		}
		if (packetalid) {
			sendRedoResponse(packet);
		}
		return (!packetalid);
	}

	final void handlePoll(PacketQueue.Packet packet) {
		if (rightRemotePollTarget(packet)) {
			remoteReq.setPacket(packet);
			doPoll(remoteReq);
			if (!remoteReq.scheduled) {
				Data oldValue = (Data) remoteReq.response;
				if (oldValue != null && oldValue.size() > 0) {
					doSet(oldValue, packet.value);
				}
				sendResponse(packet);
			} else {
				packet.returnToContainer();
			}
			remoteReq.reset();
		}
	}

	final void handleRead(PacketQueue.Packet packet) {
		Q q = getQ(packet.name);
		q.read(packet);
	}

	final void handleRemove(PacketQueue.Packet packet) {
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

		public int getOperation() {
			return OP_B_TXN_COMMIT;
		}

		public void commitPoll(String name) {
			this.name = name;
			this.txnId = ThreadContext.get().getTxnId();
			enqueueAndReturn(this);
		}

		public void handleResponse(PacketQueue.Packet packet) {
		}

		public void process() {
			MemberImpl nextMember = getNextMemberAfter(thisAddress, true, 1);
			if (nextMember != null) {
				Address next = nextMember.getAddress();
				PacketQueue.Packet packet = obtainPacket();
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

	public class Size extends AbstractCall {
		String name = null;
		int total = 0;
		int numberOfResponses = 0;
		int numberOfExpectedResponses = 1;

		public int getSize(String name) {
			this.name = name;
			synchronized (this) {
				try {
					enqueueAndReturn(this);
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			return total;
		}

		@Override
		public void onDisconnect(Address dead) {
			redo();
		}

		public void reset() {
			total = 0;
			numberOfResponses = 0;
			numberOfExpectedResponses = 0;
		}

		public void handleResponse(PacketQueue.Packet packet) {
			total += (int) packet.longValue;
			packet.returnToContainer();
			numberOfResponses++;
			if (numberOfResponses >= numberOfExpectedResponses) {
				removeCall(getId());
				synchronized (this) {
					notify();
				}
			}
		}

		public void process() {
			reset();
			Q q = getQ(name);
			if (DEBUG) {
				q.printStack();
			}
			total += q.size;
			numberOfResponses = 1;
			if (lsMembers.size() > 1) {
				addCall(this);
				numberOfExpectedResponses = lsMembers.size();
				for (MemberImpl member : lsMembers) {
					if (!member.localMember()) {
						PacketQueue.Packet packet = obtainPacket();
						packet.name = name;
						packet.operation = getOperation();
						packet.callId = getId();
						packet.timeout = 0;
						boolean sent = send(packet, member.getAddress());
						if (!sent) {
							packet.returnToContainer();
							redo();
						}
					}
				}
			} else {
				synchronized (this) {
					this.notify();
				}
			}
		}

		public int getOperation() {
			return OP_B_SIZE;
		}
	}

	class QIterator<E> implements Iterator<E>, Processable {

		volatile String name;
		volatile List<Integer> blocks = null;
		int currentBlockId = -1;
		int currentIndex = -1;
		Read read = new Read();
		Object next = null;
		boolean hasNextCalled = false;

		public void set(String name) {
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
			while (next == null) {
				boolean canRead = setNextBlock();
				// System.out.println(currentBlockId + " Can read " + canRead);
				if (!canRead)
					return false;
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

		public void handleResponse(PacketQueue.Packet packet) {
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

		public int getOperation() {
			return OP_B_REMOVE;
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
			readIndex = index;
			responses.add((value == null) ? OBJECT_NULL : value);
		}

		public void handleResponse(PacketQueue.Packet packet) {
			Data value = doTake(packet.value);
			int indexRead = (int) packet.longValue;
			setResponse(value, indexRead);
			packet.returnToContainer();
		}

		@Override
		public void onDisconnect(Address dead) {
			enqueueAndReturn(Read.this);
		}

		public void process() {
			addCall(this);
			Q q = getQ(name);

			Address target = q.getBlockOwner(blockId);
			if (target == null) {
				responses.add(OBJECT_NULL);
			} else if (target.equals(thisAddress)) {
				q.read(this);
			} else {
				PacketQueue.Packet packet = obtainPacket();
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

		public int getOperation() {
			return OP_B_READ;
		}

	}

	class AddTopicListener extends LongOp {
		public Long add(String name, Object value, long timeout, long txnId) {
			return (Long) objectCall(OP_B_ADD_TOPIC_LISTENER, name, null, value, timeout, txnId, -1);
		}

		@Override
		void setTarget() {
			target = getTargetForOffer(request);
		}

		@Override
		void doLocalOp() {
			doAddTopicListener(request);
			setResult(Long.valueOf(request.recordId));
		}
	}

	class Offer extends BooleanOp {

		public boolean publish(String name, Object value, long timeout, long txnId) {
			return booleanCall(OP_B_PUBLISH, name, null, value, timeout, txnId, -1);
		}

		public boolean offer(String name, Object value, long timeout, long txnId) {
			return booleanCall(OP_B_OFFER, name, null, value, timeout, txnId, -1);
		}

		@Override
		void handleNoneRedoResponse(PacketQueue.Packet packet) {
			if (request.operation == OP_B_OFFER
					&& packet.responseType == ResponseTypes.RESPONSE_SUCCESS) {
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
		void setTarget() {
			target = getTargetForOffer(request);
		}

		@Override
		void doLocalOp() {
			if (request.operation == OP_B_OFFER) {
				doOffer(request);
				if (!request.scheduled) {
					setResult(request.response);
				}
			} else {
				doPublish(request);
				setResult(request.response);
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
			return objectCall(OP_B_PEEK, name, null, null, 0, -1, -1);
		}

		public Object poll(String name, long timeout) {
			return objectCall(OP_B_POLL, name, null, null, timeout, -1, -1);
		}

		@Override
		void setTarget() {
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
		void doLocalOp() {
			doPoll(request);
			if (!request.scheduled) {
				setResult(request.response);
			}
		}
		
		@Override
		void handleNoneRedoResponse(PacketQueue.Packet packet) {
			if (request.operation == OP_B_POLL
					&& packet.responseType == ResponseTypes.RESPONSE_SUCCESS) {
				if (!zeroBackup) {
					if (getPreviousMemberBefore(thisAddress, true, 1).getAddress().equals(
							packet.conn.getEndPoint())) {
						if (packet.value != null ) {
							Q q = getQ (packet.name);
							q.doBackup(false, null, request.blockId, 0);
						}
					}
				}
			}
			super.handleNoneRedoResponse(packet);
		}
	}

	void doAddTopicListener(Request req) {
		for (MemberImpl member : lsMembers) {
			if (member.localMember()) {
				handleListenerRegisterations(true, req.name, req.key, req.caller, true);
			} else if (!member.getAddress().equals(req.caller)) {
				sendProcessableTo(new TopicListenerRegistration(req.name, true, req.caller), member
						.getAddress());
			}
		}
		Q q = getQ(req.name);
		if (q.blCurrentPut == null) {
			q.setCurrentPut();
		}
		req.recordId = q.getRecordId(q.blCurrentPut.blockId, q.blCurrentPut.addIndex);
	}

	public static class TopicListenerRegistration extends AbstractRemotelyProcessable {
		String name = null;
		boolean add = true;
		Address listenerAddress = null;

		public TopicListenerRegistration() {
		}

		public TopicListenerRegistration(String name, boolean add, Address listenerAddress) {
			super();
			this.name = name;
			this.add = add;
			this.listenerAddress = listenerAddress;
		}
		
		@Override
		public void readData(DataInput in) throws IOException {
			add = in.readBoolean();
			listenerAddress = new Address();
			listenerAddress.readData(in);
			name = in.readUTF();
		}
		
		@Override
		public void writeData(DataOutput out) throws IOException {
			out.writeBoolean(add);
			listenerAddress.writeData(out);
			out.writeUTF(name);
		}
 
		public void process() {
			ListenerManager.get().handleListenerRegisterations(true, name, null, listenerAddress,
					true);
		}

	}

	void doPublish(Request req) {
		Q q = getQ(req.name);
		if (q.blCurrentPut == null) {
			q.setCurrentPut();
		}
		int index = q.publish(req);
		req.longValue = index;
		req.response = Boolean.TRUE;
	}

	void doOffer(Request req) {
		Q q = getQ(req.name);
		if (q.blCurrentPut == null) {
			q.setCurrentPut();
		}
		if (q.size >= q.maxSizePerJVM) {
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
		int index = q.offer(req);
		req.value = null;		
		req.longValue = index;
		req.response = Boolean.TRUE;
	}

	void doPoll(final Request req) {
		Q q = getQ(req.name);
		if (q.blCurrentTake == null) {
			q.setCurrentTake();
		}
		if (q.blCurrentTake.size() == 0) {
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
		if (req.operation == OP_B_PEEK) {
			value = q.peek();
		} else {
			value = q.poll(req);
		}
		req.response = value;
	}

	public void sendAddBlockMessageToOthers(Block block, int fullBlockId, Address except,
			boolean add) {
		int operation = OP_B_ADD_BLOCK;
		if (!add) {
			operation = OP_B_REMOVE_BLOCK;
		}
		if (lsMembers.size() > 1) {
			int addBlockId = -1;
			int removeBlockId = -1;
			Address addAddress = null;
			if (add) {
				addBlockId = block.blockId;
				addAddress = block.address;
				except = null;
			} else {
				removeBlockId = block.blockId;
			}
			BlockUpdate addRemove = new BlockUpdate(addAddress, addBlockId, fullBlockId,
					removeBlockId);
			for (MemberImpl member : lsMembers) {
				if (!member.localMember()) {
					Address dest = member.getAddress();
					if (!dest.equals(except)) {
						send(block.name, operation, addRemove, dest);
					}
				}
			}
		}
	}

	public void sendFullMessage(Block block) {
		try {
			PacketQueue.Packet packet = PacketQueue.get().obtainPacket();
			packet.set(block.name, OP_B_FULL_BLOCK, null, null);
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
		int size = 0;
		List<Block> lsBlocks = new ArrayList<Block>(10);
		Block blCurrentPut = null;
		Block blCurrentTake = null;
		int latestAddedBlock = -1;

		List<ScheduledPollAction> lsScheduledPollActions = new ArrayList<ScheduledPollAction>(100);
		List<ScheduledOfferAction> lsScheduledOfferActions = new ArrayList<ScheduledOfferAction>(
				100);
		int maxSizePerJVM = Integer.MAX_VALUE;

		Map<Address, Boolean> mapListeners = new HashMap<Address, Boolean>(1);
		boolean keepValues = true;

		public Q(String name) {
			if (name.startsWith("q:t:")) {
				keepValues = false;
			} else {
				QConfig qconfig = Config.get().getQConfig(name.substring(2));
				if (qconfig != null) {
					maxSizePerJVM = qconfig.maxSizePerJVM;
					log("qConfig " + qconfig.maxSizePerJVM);
				}
			}
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
			return new Block(address, blockId, name, keepValues);
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
				Data value = poll(request);
				request.response = value;
				request.key = null;
				request.value = null;
				returnScheduledAsSuccess(request);
				return true;
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
				offer(request);
				request.response = Boolean.TRUE;
				request.key = null;
				request.value = null;
				returnScheduledAsBoolean(request);
				return true;
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
			int size = lsBlocks.size();
			for (int i = 0; i < size; i++) {
				Block b = lsBlocks.get(i);
				if (b.blockId == blockId) {
					blCurrentTake = null;
					return lsBlocks.remove(i);
				}
			}
			return null;
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

		public Block getCurrentPutBlock() {
			if (blCurrentPut == null) {
				setCurrentPut();
			}
			return blCurrentPut;
		}

		Block getCurrentTakeBlock() {
			if (blCurrentTake == null) {
				if (lsBlocks.size() == 0) {
					return null;
				}
				blCurrentTake = lsBlocks.get(0);
			}
			return blCurrentTake;
		}

		public void setCurrentTake() {
			if (blCurrentTake == null) {
				blCurrentTake = lsBlocks.get(0);
				// System.out.println("CurrentTake : " + blCurrentTake);
			}
		}

		void remove(Remove remove) {
			Block block = getBlock(remove.blockId);
			if (block != null) {
				block.remove(remove.index);
			}
			remove.setResponse();
		}

		void remove(PacketQueue.Packet packet) {
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

		void read(PacketQueue.Packet packet) {
			int index = (int) packet.longValue;
			int blockId = packet.blockId;
			Block block = getBlock(blockId);
			packet.longValue = -1;
			if (block != null) {
				blockLoop: for (int i = index; i < BLOCK_SIZE; i++) {
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

		void doFireEntryEvent(boolean add, Data value, long recordId) {
			if (mapListeners.size() == 0)
				return;
			if (add) {
				fireMapEvent(mapListeners, name, EntryEvent.TYPE_ADDED, value, null, recordId);
			} else {
				fireMapEvent(mapListeners, name, EntryEvent.TYPE_REMOVED, value, null, recordId);
			}
		}

		long getRecordId(int blockId, int addIndex) {
			return (blockId * BLOCK_SIZE) + addIndex;
		}

		int publish(Request req) {
			int addIndex = blCurrentPut.add(req.value);
			long recordId = getRecordId(blCurrentPut.blockId, addIndex);
			doFireEntryEvent(true, req.value, recordId);

			if (blCurrentPut.isFull()) {
				fireBlockFullEvent(blCurrentPut);
				blCurrentPut = null;
				setCurrentPut();
			}
			return addIndex;
		}

		int offer(Request req) {
			int addIndex = blCurrentPut.add(req.value);
			long recordId = getRecordId(blCurrentPut.blockId, addIndex);
			req.recordId = recordId;
			doFireEntryEvent(true, req.value, recordId);
			size++;
			while (lsScheduledPollActions.size() > 0) {
				ScheduledAction pollAction = lsScheduledPollActions.remove(0);
				ClusterManager.get().deregisterScheduledAction(pollAction);
				if (pollAction.expired()) {
					pollAction.onExpire();
				} else {
					boolean consumed = pollAction.consume();
					if (consumed)
						return -1;
				}
			}
			sendBackup(true, req.caller, req.value, blCurrentPut.blockId, addIndex);
			if (blCurrentPut.isFull()) {
				fireBlockFullEvent(blCurrentPut);
				blCurrentPut = null;
				setCurrentPut();
			}
			return addIndex;
		}

		public Data peek() {
			setCurrentTake();
			if (blCurrentTake == null)
				return null;
			Data value = blCurrentTake.peek();
			if (value == null) {
				return null;
			}
			return ThreadContext.get().hardCopy(value);
		}

		public Data poll(Request request) {
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
			size--;
			long recordId = getRecordId(blCurrentTake.blockId, blCurrentTake.removeIndex);
			request.recordId = recordId;
			doFireEntryEvent(false, value, recordId);
			runScheduledOffer: while (lsScheduledOfferActions.size() > 0) {
				ScheduledOfferAction offerAction = lsScheduledOfferActions.remove(0);
				ClusterManager.get().deregisterScheduledAction(offerAction);
				if (offerAction.expired()) {
					offerAction.onExpire();
				} else {
					boolean consumed = offerAction.consume();
					if (consumed) {
						break runScheduledOffer;
					}
				}
			}
			sendBackup(false, request.caller, null, blCurrentTake.blockId, 0);
			if (blCurrentTake.size() == 0 && blCurrentTake.isFull()) {
				fireBlockRemoveEvent(blCurrentTake);
				blCurrentTake = null;
			}
			return value;
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
			PacketQueue.Packet packet = obtainPacket(name, null, value, OP_B_TXN_BACKUP_POLL, 0);
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
				int operation = OP_B_BACKUP_REMOVE;
				if (add) {
					operation = OP_B_BACKUP_ADD;
					if (data != null && data.size() > 0) {
						data = ThreadContext.get().hardCopy(data);
					}
				}
				PacketQueue.Packet packet = obtainPacket(name, null, data, operation, 0);
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

		void setCurrentPut() {
			int size = lsBlocks.size();
			for (int i = 0; i < size; i++) {
				Block block = lsBlocks.get(i);
				// System.out.println("setCurrentPut check " + block);
				if (!block.isFull()) {
					blCurrentPut = block;
					// System.out.println("Setting..currentPut " +
					// blCurrentPut);
					return;
				}
			}
		}

		void printStack() {
			if (DEBUG) {
				log("====================");
				for (Block block : lsBlocks) {
					log(block);
				}
				log("=====================");
				log("CurrenTake " + blCurrentTake);
				log("CurrentPut " + blCurrentPut);
				log("---------------------");
			}
		}
	}

	class Block {
		final int blockId;
		final String name;
		Address address;
		int addIndex = 0;
		int removeIndex = 0;
		Data[] values = null;
		int size = 0;
		boolean full = false;

		public Block(Address address, int blockId, String name, boolean keepValues) {
			super();
			this.address = address;
			this.blockId = blockId;
			this.name = name;
			if (keepValues)
				values = new Data[BLOCK_SIZE];
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
			while (index > 0) {
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
			if (values != null) {
				if (values[addIndex] != null)
					return -1;
				values[addIndex] = data;
			}
			size++;
			int addedCurrentIndex = addIndex;
			addIndex++;
			if (addIndex >= BLOCK_SIZE) {
				full = true;
			}
			return addedCurrentIndex;
		}

		public boolean add(int index, Data data) {
			if (values[index] != null)
				return false;
			values[index] = data;
			size++;
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
			if (value != null)
				size--;
			return value;
		}

		public Data remove() {
			for (int i = removeIndex; i < BLOCK_SIZE; i++) {
				if (values[i] != null) {
					Data value = values[i];
					values[i] = null;
					size--;
					removeIndex = i;
					return value;
				}
			}
			return null;
		}

		public int size() {
			return size;
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
