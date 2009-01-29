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

import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_ADD_TO_LIST;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_ADD_TO_SET;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_BACKUP_ADD;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_BACKUP_LOCK;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_BACKUP_REMOVE;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_BLOCKS;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_BLOCK_INFO;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_CONTAINS_KEY;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_CONTAINS_VALUE;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_GET;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_LOCK;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_LOCK_RETURN_OLD;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_MIGRATE_RECORD;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_MIGRATION_COMPLETE;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_PUT;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_PUT_IF_ABSENT;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_READ;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_REMOVE;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_REMOVE_IF_SAME;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_REPLACE_IF_NOT_NULL;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_SIZE;
import static com.hazelcast.impl.Constants.ConcurrentMapOperations.OP_CMAP_UNLOCK;
import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import static com.hazelcast.impl.Constants.Timeouts.DEFAULT_TXN_TIMEOUT;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Transaction;
import com.hazelcast.impl.ClusterManager.AbstractRemotelyProcessable;
import com.hazelcast.impl.FactoryImpl.IProxy;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.InvocationQueue.Data;
import com.hazelcast.nio.InvocationQueue.Invocation;

class ConcurrentMapManager extends BaseManager {
	private static ConcurrentMapManager instance = new ConcurrentMapManager();

	private ConcurrentMapManager() {
	}

	public static ConcurrentMapManager get() {
		return instance;
	}

	private static final int BLOCK_COUNT = 100;

	private Request remoteReq = new Request();
	private Map<Integer, Block> mapBlocks = new HashMap<Integer, Block>(BLOCK_COUNT);
	private Map<String, CMap> maps = new HashMap<String, CMap>(10);
	private long maxId = 0;
	private Map<Long, Record> mapRecordsById = new HashMap<Long, Record>();

	public void handle(Invocation inv) {
		if (inv.operation == OP_CMAP_GET) {
			handleGet(inv);
		} else if (inv.operation == OP_CMAP_PUT) {
			handlePut(inv);
		} else if (inv.operation == OP_CMAP_BACKUP_ADD) {
			handleBackupAdd(inv);
		} else if (inv.operation == OP_CMAP_REMOVE) {
			handleRemove(inv);
		} else if (inv.operation == OP_CMAP_BACKUP_REMOVE) {
			handleBackupRemove(inv);
		} else if (inv.operation == OP_CMAP_LOCK) {
			handleLock(inv);
		} else if (inv.operation == OP_CMAP_UNLOCK) {
			handleLock(inv);
		} else if (inv.operation == OP_CMAP_BACKUP_LOCK) {
			handleBackupLock(inv);
		} else if (inv.operation == OP_CMAP_READ) {
			handleRead(inv);
		} else if (inv.operation == OP_CMAP_SIZE) {
			handleSize(inv);
		} else if (inv.operation == OP_CMAP_ADD_TO_LIST || inv.operation == OP_CMAP_ADD_TO_SET) {
			handleAdd(inv);
		} else if (inv.operation == OP_CMAP_CONTAINS_KEY) {
			handleContains(true, inv);
		} else if (inv.operation == OP_CMAP_CONTAINS_VALUE) {
			handleContains(false, inv);
		} else if (inv.operation == OP_CMAP_BLOCK_INFO) {
			handleBlockInfo(inv);
		} else if (inv.operation == OP_CMAP_BLOCKS) {
			handleBlocks(inv);
		} else if (inv.operation == OP_CMAP_PUT_IF_ABSENT) {
			handlePut(inv);
		} else if (inv.operation == OP_CMAP_REPLACE_IF_NOT_NULL) {
			handlePut(inv);
		} else if (inv.operation == OP_CMAP_MIGRATION_COMPLETE) {
			doMigrationComplete(inv.conn.getEndPoint());
		} else if (inv.operation == OP_CMAP_MIGRATE_RECORD) {
			handleMigrateRecord(inv);
		} else {
			throw new RuntimeException("Unknown operation " + inv.operation);
		}
	}

	public void syncForDead(Address deadAddress) {
		Collection<Block> blocks = mapBlocks.values();
		for (Block block : blocks) {
			if (deadAddress.equals(block.owner)) {
				MemberImpl member = getNextMemberBeforeSync(block.owner, true, 1);
				Address addressNewOwner = (member == null) ? thisAddress : member.getAddress();
				block.owner = addressNewOwner;
			}
			if (block.migrationAddress != null) {
				if (deadAddress.equals(block.migrationAddress)) {
					MemberImpl member = getNextMemberBeforeSync(block.migrationAddress, true, 1);
					Address addressNewMigrationAddress = (member == null) ? thisAddress : member
							.getAddress();
					block.migrationAddress = addressNewMigrationAddress;
				}
			}
		}
		Collection<CMap> cmaps = maps.values();
		for (CMap map : cmaps) {
			Collection<Record> records = map.mapRecords.values();
			for (Record record : records) {
				record.onDisconnect(deadAddress);
			}
		}
		doResetRecords();
	}

	public void syncForAdd() {
		if (isMaster()) {
			// make sue that all blocks are actually created
			for (int i = 0; i < BLOCK_COUNT; i++) {
				Block block = mapBlocks.get(i);
				if (block == null) {
					getOrCreateBlock(i);
				}
			}

			List<Block> lsBlocksToRedistribute = new ArrayList<Block>();
			Map<Address, Integer> addressBlocks = new HashMap<Address, Integer>();
			int storageEnabledMemberCount = 0;
			for (MemberImpl member : lsMembers) {
				if (!member.superClient()) {
					addressBlocks.put(member.getAddress(), 0);
					storageEnabledMemberCount++;
				}
			}
			if (storageEnabledMemberCount == 0)
				return;
			int aveBlockOwnCount = mapBlocks.size() / (storageEnabledMemberCount);
			Collection<Block> blocks = mapBlocks.values();
			for (Block block : blocks) {
				if (block.owner == null) {
					lsBlocksToRedistribute.add(block);
				} else {
					if (!block.isMigrating()) {
						Integer countInt = addressBlocks.get(block.owner);
						int count = (countInt == null) ? 0 : countInt.intValue();
						if (count >= aveBlockOwnCount) {
							lsBlocksToRedistribute.add(block);
						} else {
							count++;
							addressBlocks.put(block.owner, count);
						}
					}
				}
			}

			Set<Address> allAddress = addressBlocks.keySet();
			setNewMembers: for (Address address : allAddress) {
				Integer countInt = addressBlocks.get(address);
				int count = (countInt == null) ? 0 : countInt.intValue();
				while (count < aveBlockOwnCount) {
					if (lsBlocksToRedistribute.size() > 0) {
						Block blockToMigrate = lsBlocksToRedistribute.remove(0);
						if (blockToMigrate.owner == null) {
							blockToMigrate.owner = address;
						} else {
							blockToMigrate.migrationAddress = address;
							if (blockToMigrate.owner.equals(blockToMigrate.migrationAddress)) {
								blockToMigrate.migrationAddress = null;
							}
						}
						count++;
					} else {
						break setNewMembers;
					}
				}
			}
			Data dataAllBlocks = null;
			for (MemberImpl member : lsMembers) {
				if (!member.localMember()) {
					if (dataAllBlocks == null) {
						Blocks allBlocks = new Blocks();
						blocks = mapBlocks.values();
						for (Block block : blocks) {
							allBlocks.addBlock(block);
						}
						dataAllBlocks = ThreadContext.get().toData(allBlocks);
					}
					send("blocks", OP_CMAP_BLOCKS, dataAllBlocks, member.getAddress());
				}
			}
			doResetRecords();
			if (DEBUG) {
				printBlocks();
			}
		}
	}

	abstract class MBooleanOp extends MTargetAwareOp {
		@Override
		void handleNoneRedoResponse(Invocation inv) {
			handleBooleanNoneRedoResponse(inv);
		}
	}

	class ScheduledLockAction extends ScheduledAction {
		Record record = null;

		public ScheduledLockAction(Request reqScheduled, Record record) {
			super(reqScheduled);
			this.record = record;
		}

		@Override
		public boolean consume() {
			if (DEBUG) {
				log("consuming scheduled lock ");
			}
			record.lock(request.lockThreadId, request.lockAddress);
			sendLockBackup(record);
			request.response = Boolean.TRUE;
			returnScheduledAsBoolean(request);
			return true;
		}

		public void onExpire() {
			request.response = Boolean.FALSE;
			returnScheduledAsBoolean(request);
		}
	}

	class MLock extends MBooleanOp {
		Data oldValue = null;

		public boolean unlock(String name, Object key, long timeout, long txnId) {
			return booleanCall(OP_CMAP_UNLOCK, name, key, null, timeout, txnId, -1);
		}

		public boolean lock(String name, Object key, long timeout, long txnId) {
			return booleanCall(OP_CMAP_LOCK, name, key, null, timeout, txnId, -1);
		}

		public boolean lockAndReturnOld(String name, Object key, long timeout, long txnId) {
			oldValue = null;
			return booleanCall(OP_CMAP_LOCK_RETURN_OLD, name, key, null, timeout, txnId, -1);
		}

		@Override
		public void doLocalOp() {
			doLock(request);
			if (!request.scheduled) {
				responses.add(request.response);
			}
			oldValue = request.value;
		}

		@Override
		void handleNoneRedoResponse(Invocation inv) {
			if (request.operation == OP_CMAP_LOCK_RETURN_OLD) {
				oldValue = inv.doTake(inv.data);
			}
			super.handleNoneRedoResponse(inv);
		}
	}

	class MContainsKey extends MBooleanOp {
		public boolean containsKey(String name, Object key, long txnId) {
			return booleanCall(OP_CMAP_CONTAINS_KEY, name, key, null, 0, txnId, -1);
		}

		@Override
		public void doLocalOp() {
			doContains(true, request);
			setResult(request.response);
		}
	}

	class MIterator implements Iterator {

		volatile String name;
		volatile List<Integer> blocks = null;
		int currentBlockId = -1;
		long currentIndex = -1;
		MRead read = new MRead();
		SimpleDataEntry next = null;
		boolean hasNextCalled = false;
		final static int TYPE_ENTRIES = 1;
		final static int TYPE_KEYS = 2;
		final static int TYPE_VALUES = 3;

		int type = TYPE_ENTRIES;

		public void set(String name, int type) {
			this.name = name;
			this.type = type;
		}

		public boolean hasNext() {
			if (next != null) {
				if (next.copyCount-- <= 0) {
					next = null;
				}
			}

			while (next == null) {
				boolean canRead = setNextBlock();
				if (!canRead)
					return false;
				next = read.read(name, currentBlockId, currentIndex);
				if (next == null) {
					currentIndex = -1;
				} else {
					currentIndex = read.lastReadRecordId;
					currentIndex++;
				}
			}
			hasNextCalled = true;
			return true;
		}

		boolean setNextBlock() {
			if (currentIndex == -1) {
				currentBlockId++;
				if (currentBlockId >= BLOCK_COUNT) {
					return false;
				}
				currentIndex = 0;
				return true;
			}
			return true;
		}

		public Object next() {
			if (!hasNextCalled) {
				boolean hasNext = hasNext();
				if (!hasNext) {
					return null;
				}
			}
			if (next != null) {
				hasNextCalled = false;
			}
			if (type == TYPE_ENTRIES)
				return next;
			else if (type == TYPE_KEYS)
				return next.getKey();
			else
				return next.getValue();
		}

		public void remove() {
			if (next != null) {
				IProxy iproxy = (IProxy) FactoryImpl.getProxy(name);
				iproxy.removeKey(next.getKeyData());
			}
		}
	}

	class MMigrate extends MBooleanOp {
		public boolean migrate(long recordId) {
			request.recordId = recordId;
			doOp();
			return getResultAsBoolean();
		}

		@Override
		void setTarget() {
			target = thisAddress;
			Record record = getRecordById(request.recordId);
			if (record == null) {
				logger.log(Level.FINEST, "record is null " + request.recordId);
				return;
			} else {
				Block block = mapBlocks.get(record.blockId);
				if (block.migrationAddress == null) {
					logger.log(Level.FINEST, "not migrating " + request.recordId);
					return;
				} else {
					target = block.migrationAddress;
				}
				setLocal(OP_CMAP_MIGRATE_RECORD, record.name, record.key, record.value, 0, -1,
						record.id);
			}
		}

		@Override
		void doLocalOp() {
			setResult(Boolean.FALSE);
		}
	}

	class MRead extends MTargetAwareOp {
		long lastReadRecordId = 0;
		boolean containsValue = true;

		public SimpleDataEntry read(String name, int blockId, long recordId) {
			setLocal(OP_CMAP_READ, name, null, null, 0, -1, recordId);
			request.blockId = blockId;
			return (SimpleDataEntry) objectCall();
		}

		@Override
		void doLocalOp() {
			doRead(request);
			lastReadRecordId = request.recordId;
			if (request.response == null) {
				setResult(null);
			} else {
				Record record = (Record) request.response;
				request.recordId = record.id;
				lastReadRecordId = request.recordId;
				Data key = ThreadContext.get().hardCopy(record.getKey());
				Data value = null;
				if (containsValue) {
					value = ThreadContext.get().hardCopy(record.getValue());
				}
				setResult(new SimpleDataEntry(request.name, request.blockId, key, value, record
						.getCopyCount()));
			}
		}

		@Override
		void handleNoneRedoResponse(Invocation inv) {
			removeCall(getId());
			lastReadRecordId = inv.recordId;
			Data key = inv.doTake(inv.key);
			if (key == null) {
				setResult(null);
			} else {
				Data value = null;
				if (containsValue) {
					value = inv.doTake(inv.data);
				}
				setResult(new SimpleDataEntry(request.name, request.blockId, key, value,
						(int) inv.longValue));
			}
		}

		@Override
		void setTarget() {
			setTargetBasedOnBlockId();
		}
	}

	class MGet extends MTargetAwareOp {
		public Object get(String name, Object key, long timeout, long txnId) {
			return objectCall(OP_CMAP_GET, name, key, null, timeout, txnId, -1);
		}

		@Override
		public void doLocalOp() {
			doGet(request);
			Data value = (Data) request.response;
			if (value != null) {
				value = ThreadContext.get().hardCopy(value);
			}
			setResult(value);
		}
	}

	class MRemove extends MTargetAwareOp {

		public Object remove(String name, Object key, long timeout, long txnId) {
			return objectCall(OP_CMAP_REMOVE, name, key, null, timeout, txnId, -1);
		}

		public Object removeIfSame(String name, Object key, Object value, long timeout, long txnId) {
			return objectCall(OP_CMAP_REMOVE_IF_SAME, name, key, value, timeout, txnId, -1);
		}

		@Override
		public void doLocalOp() {
			doRemove(request);
			if (!request.scheduled) {
				setResult(request.response);
			}
		}
	}

	class MAdd extends MTargetAwareOp {
		boolean addToList(String name, Object value) {
			Data key = ThreadContext.get().toData(value);
			return booleanCall(OP_CMAP_ADD_TO_LIST, name, key, null, 0, -1, -1);
		}

		boolean addToSet(String name, Object value) {
			Data key = ThreadContext.get().toData(value);
			return booleanCall(OP_CMAP_ADD_TO_SET, name, key, null, 0, -1, -1);
		}

		@Override
		void doLocalOp() {
			doAdd(request);
			setResult(request.response);
		}
	}

	class MPut extends MTargetAwareOp {

		public Object replace(String name, Object key, Object value, long timeout, long txnId) {
			return txnalPut(OP_CMAP_REPLACE_IF_NOT_NULL, name, key, value, timeout, txnId);
		}

		public Object putIfAbsent(String name, Object key, Object value, long timeout, long txnId) {
			return txnalPut(OP_CMAP_PUT_IF_ABSENT, name, key, value, timeout, txnId);
		}

		public Object put(String name, Object key, Object value, long timeout, long txnId) {
			return txnalPut(OP_CMAP_PUT, name, key, value, timeout, txnId);
		}

		private Object txnalPut(int operation, String name, Object key, Object value, long timeout,
				long txnId) {
			ThreadContext threadContext = ThreadContext.get();
			TransactionImpl txn = threadContext.txn;
			if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
				try {
					boolean locked = false;
					if (!txn.has(name, key)) {
						MLock mlock = threadContext.getMLock();
						locked = mlock
								.lockAndReturnOld(name, key, DEFAULT_TXN_TIMEOUT, txn.getId());
						if (!locked)
							throwCME(key);
						Object oldObject = null;
						Data oldValue = mlock.oldValue;
						if (oldValue != null) {
							oldObject = threadContext.toObject(oldValue);
						}
						txn.attachPutOp(name, key, value, (oldObject == null));
						return oldObject;
					} else {
						return txn.attachPutOp(name, key, value, false);
					}
				} catch (Exception e1) {
					e1.printStackTrace();
				}
				return null;
			} else {
				return objectCall(operation, name, key, value, timeout, txnId, -1);
			}
		}

		@Override
		public void doLocalOp() {
			doPut(request);
			if (!request.scheduled) {
				setResult(request.response);
			}
		}

		@Override
		public void handleNoneRedoResponse(Invocation inv) {
			if (inv.responseType == ResponseTypes.RESPONSE_SUCCESS) {
				if (!zeroBackup) {
					if (getPreviousMemberBefore(thisAddress, true, 1).getAddress().equals(
							inv.conn.getEndPoint())) {
						// so I am the backup so
						CMap cmap = getMap(request.name);
						// inv endpoint is the actual owner of the
						// record. so set it before backup
						request.caller = inv.conn.getEndPoint();
						cmap.backupAdd(request);
					}
				}
			}
			super.handleNoneRedoResponse(inv);
		}
	}

	abstract class MTargetAwareOp extends TargetAwareOp {
		@Override
		void setTarget() {
			setTargetBasedOnKey();
		}

		final void setTargetBasedOnKey() {
			if (target != null) {
				Block block = getBlock(request.key);
				if (target.equals(block.owner)) {
					target = block.migrationAddress;
				} else if (target.equals(block.migrationAddress)) {
					if (isMaster()) {
						target = block.owner;
					}
				} else {
					target = null;
				}
			}
			if (target == null) {
				target = getTarget(request.name, request.key);
			}
		}

		final void setTargetBasedOnBlockId() {
			Block block = mapBlocks.get(request.blockId);
			if (block == null) {
				target = thisAddress;
				return;
			}
			if (block.isMigrating()) {
				target = block.migrationAddress;
			} else {
				target = block.owner;
			}
		}
	}

	public Address getKeyOwner(String name, Data key) {
		return getTarget(name, key);
	}

	public class MContainsValue extends AllOp {
		boolean contains = false;

		public boolean containsValue(String name, Object value, long txnId) {
			contains = false;
			reset();
			setLocal(OP_CMAP_CONTAINS_VALUE, name, null, value, 0, txnId, -1);
			doOp();
			return contains;
		}

		@Override
		public void process() {
			doContains(false, request);
			if (request.response == Boolean.TRUE) {
				contains = true;
				complete(false);
				return;
			}
			super.process();
		}

		@Override
		void doLocalOp() {
		}

		@Override
		public void handleResponse(Invocation inv) {
			if (inv.responseType == ResponseTypes.RESPONSE_SUCCESS) {
				contains = true;
				complete(true);
			}
			consumeResponse(inv);
		}
	}

	public class MSize extends AllOp {
		int total = 0;
		volatile boolean shouldRedo = false;

		public int getSize(String name) {
			reset();
			shouldRedo = false;
			total = 0;
			setLocal(OP_CMAP_SIZE, name, null, null, 0, -1, -1);
			doOp();
			if (shouldRedo) {				 
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				return getSize (name);
			}
			return total;
		}
		
		@Override
		public void process() {
			Collection<Block> blocks = mapBlocks.values();
			for (Block block : blocks) {
				if (block.isMigrating()) {
					shouldRedo = true;
					complete(true);
					return;
				}
			}
			super.process();
		}

		@Override
		void doLocalOp() {
			CMap cmap = getMap(request.name);
			total += cmap.size();
		}

		@Override
		public void handleResponse(Invocation inv) {
			total += (int) inv.longValue;
			consumeResponse(inv);
		}
	}

	void handleSize(Invocation inv) {
		CMap cmap = getMap(inv.name);
		inv.longValue = cmap.size();
		// if (DEBUG) {
		// printBlocks();
		// }
		sendResponse(inv);
	}

	public Block getBlock(Data key) {
		int blockId = getBlockId(key);
		return mapBlocks.get(blockId);
	}

	Address getTarget(String name, Data key) {
		int blockId = getBlockId(key);
		Block block = mapBlocks.get(blockId);
		if (block == null) {
			if (isMaster() && !isSuperClient()) {
				block = getOrCreateBlock(blockId);
				block.owner = thisAddress;
			} else
				return null;
		}
		if (block.owner == null)
			return null;
		if (block.owner.equals(thisAddress)) {
			if (block.isMigrating()) {
				if (name == null)
					return block.migrationAddress;
				CMap map = getMap(name);
				Record record = map.getRecord(key);
				if (record == null)
					return block.migrationAddress;
				else {
					Address recordOwner = record.getOwner();
					if (recordOwner == null)
						return thisAddress;
					if ((!recordOwner.equals(thisAddress))
							&& (!recordOwner.equals(block.migrationAddress))) {
						record.setOwner(thisAddress);
					}
					return record.getOwner();
				}
			}
		} else if (thisAddress.equals(block.migrationAddress)) {
			if (name == null)
				return thisAddress;
			CMap map = getMap(name);
			Record record = map.getRecord(key);
			if (record == null)
				return thisAddress;
			else
				return record.getOwner();
		}
		return block.owner;
	}

	private int getBlockId(Data key) {
		int hash = key.hashCode();
		return Math.abs(hash) % BLOCK_COUNT;
	}

	Block getOrCreateBlock(Data key) {
		return getOrCreateBlock(getBlockId(key));
	}

	Block getOrCreateBlock(int blockId) {
		Block block = mapBlocks.get(blockId);
		if (block == null) {
			block = new Block(blockId, null);
			mapBlocks.put(blockId, block);
		}
		return block;
	}

	CMap getMap(String name) {
		CMap map = maps.get(name);
		if (map == null) {
			map = new CMap(name);
			maps.put(name, map);
		}
		return map;
	}

	@Override
	void handleListenerRegisterations(boolean add, String name, Data key, Address address,
			boolean includeValue) {
		CMap cmap = getMap(name);
		if (add) {
			cmap.addListener(key, address, includeValue);
		} else {
			cmap.removeListener(key, address);
		}
	}

	// master should call this method
	void sendBlockInfo(Block block, Address address) {
		send("mapblock", OP_CMAP_BLOCK_INFO, block, address);
	}

	void doBackupRemoveBlock(int blockId, Address oldOwner) {
		Collection<CMap> colMaps = maps.values();
		for (CMap cmap : colMaps) {
			Collection<Record> records = cmap.mapRecords.values();
			List<Record> lsRecordToRemove = new ArrayList<Record>();
			for (Record record : records) {
				if (record.blockId == blockId) {
					if (record.owner.equals(oldOwner)) {
						lsRecordToRemove.add(record);
					}
				}
			}
			if (DEBUG) {
				log("doBAckupRemove block removing " + lsRecordToRemove.size());
			}
			for (Record record : lsRecordToRemove) {
				cmap.removeRecord(record.getKey());
			}
		}
	}

	void handleBackupRemove(Invocation inv) {
		CMap cmap = getMap(inv.name);
		cmap.removeRecord(inv.key);
		inv.returnToContainer();
	}

	void handleBackupAdd(Invocation inv) {
		CMap cmap = getMap(inv.name);
		remoteReq.setInvocation(inv);
		cmap.backupAdd(remoteReq);
		remoteReq.reset();
		inv.returnToContainer();
	}

	void handleBackupLock(Invocation inv) {
		CMap cmap = getMap(inv.name);
		remoteReq.setInvocation(inv);
		cmap.backupLock(remoteReq);
		remoteReq.reset();
		inv.returnToContainer();
	}

	void handleBlocks(Invocation inv) {
		Blocks blocks = (Blocks) ThreadContext.get().toObject(inv.data);
		handleBlocks(blocks);
		inv.returnToContainer();
		doResetRecords();
		if (DEBUG) {
			// printBlocks();
		}
	}

	void handleBlocks(Blocks blocks) {
		List<Block> lsBlocks = blocks.lsBlocks;
		for (Block block : lsBlocks) {
			doBlockInfo(block);
		}
	}

	void printBlocks() {
		if (true)
			return;
		if (DEBUG)
			log("=========================================");
		for (int i = 0; i < BLOCK_COUNT; i++) {
			if (DEBUG)
				log(mapBlocks.get(i));
		}
		Collection<CMap> cmaps = maps.values();
		for (CMap cmap : cmaps) {
			if (DEBUG)
				log(cmap);
		}
		if (DEBUG)
			log("=========================================");

	}

	void handleBlockInfo(Invocation inv) {
		Block blockInfo = (Block) ThreadContext.get().toObject(inv.data);
		doBlockInfo(blockInfo);
		inv.returnToContainer();
	}

	void doBlockInfo(Block blockInfo) {
		Block block = mapBlocks.get(blockInfo.blockId);
		if (block == null) {
			block = blockInfo;
			mapBlocks.put(block.blockId, block);
		} else {
			if (thisAddress.equals(block.owner)) {
				// I am already owner!
				if (block.isMigrating()) {
					if (!block.migrationAddress.equals(blockInfo.migrationAddress)) {
						throw new RuntimeException();
					}
				} else {
					if (blockInfo.migrationAddress != null) {
						// I am being told to migrate
						block.migrationAddress = blockInfo.migrationAddress;
					}
				}
			} else {
				block.owner = blockInfo.owner;
				block.migrationAddress = blockInfo.migrationAddress;
			}
		}
		if (block.owner != null && block.owner.equals(block.migrationAddress)) {
			block.migrationAddress = null;
		}
	}

	class Migrator implements Runnable {
		final Set<Long> recordsToMigrate;
		final Set<Long> recordsToBackup;

		public Migrator(Set<Long> recordsToBackup, Set<Long> recordsToMigrate) {
			super();
			this.recordsToBackup = recordsToBackup;
			this.recordsToMigrate = recordsToMigrate;
		}

		public void run() {
			logger.log(Level.FINEST, "Migration started!");
			
			CheckAllConnectionsOp checkAllConnectionsOp = new CheckAllConnectionsOp();
			checkAllConnectionsOp.check();
			
			for (final Long recordId : recordsToMigrate) {
				MMigrate mmigrate = new MMigrate();
				boolean migrated = mmigrate.migrate(recordId);
				if (!migrated) {
					logger.log(Level.FINEST, "couldn't migrate " + recordId);
				}
			}

			Processable removeMigratedRecordsProcess = new Processable() {
				public void process() {
					for (final Long recordId : recordsToMigrate) {
						Record record = getRecordById(recordId);
						if (record != null) {
							Block block = getBlock(record.key);
							if (!backupMemberFor(block)) {
								CMap cmap = getMap(record.name);
								cmap.removeRecord(record.key);
							}
						}
					}
				}
			};
			enqueueAndReturn(removeMigratedRecordsProcess);

			if (recordsToBackup != null) {
				int count = 0;
				MBackupOp mbackupOp = new MBackupOp();
				for (final Long recordId : recordsToBackup) {
					Record record = getRecordById(recordId);
					if (record != null) {
						mbackupOp.backup(record);
						if (count++ > 10) {
							count = 0;
							try {
								Thread.sleep(1);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
					}

				}
			}
			Processable processCompletion = new Processable() {
				public void process() {
					doMigrationComplete(thisAddress);
					sendMigrationComplete();
					if (DEBUG) {
						printBlocks();
						logger.log(Level.FINEST, "Migration ended!");
					}

				}
			};
			enqueueAndReturn(processCompletion);
		}
	}

	void doResetRecords() {
		if (isSuperClient())
			return;
		Set<Long> recordsToMigrate = new HashSet<Long>(1000);
		Set<Long> recordsToBackup = new HashSet<Long>(1000);
		Set<Long> recordsToRemove = new HashSet<Long>(1000);
		int sendOwn = 0;
		int removed = 0;
		int sendBackup = 0;
		int migrationOwner = 0;
		int ownerBackup = 0;
		int migrationBackup = 0;
		try {
			Collection<Record> colRecords = mapRecordsById.values();
			for (Record rec : colRecords) {
				Block block = mapBlocks.get(rec.getBlockId());
				if (rec.getId() == 972) {
					System.out.println("972 block " + block);
					System.out.println("972 record " + rec);
				}
				if (block.owner.equals(thisAddress)) {
					if (block.isMigrating()) {
						// migrate
						recordsToMigrate.add(rec.getId());
						sendOwn++;
					} else {
						rec.setOwner(thisAddress);
						// backup
						recordsToBackup.add(rec.getId());
						sendBackup++;
					}
				} else if (block.isMigrating() && block.migrationAddress.equals(thisAddress)) {
					rec.owner = thisAddress;
					migrationOwner++;
				} else {
					// am I the backup!
					if (block.isMigrating()) {
						MemberImpl nextAfterMigration = getNextMemberAfter(block.migrationAddress,
								true, 1);
						if (nextAfterMigration != null
								&& nextAfterMigration.getAddress().equals(thisAddress)) {
							rec.owner = block.migrationAddress;
							migrationBackup++;
						} else {
							recordsToRemove.add(rec.getId());
						}
					} else {
						// not migrating..
						MemberImpl nextAfterOwner = getNextMemberAfter(block.owner, true, 1);
						if (nextAfterOwner != null
								&& nextAfterOwner.getAddress().equals(thisAddress)) {
							rec.owner = block.owner;
							ownerBackup++;
						} else {
							recordsToRemove.add(rec.getId());
						}
					}
				}
			}
			for (Long recordId : recordsToRemove) {
				Record rec = getRecordById(recordId);
				if (rec != null) {
					if (rec.getId() == 972) {
						System.out.println("972 removing " + rec);
					}
					CMap cmap = getMap(rec.name);
					cmap.removeRecord(rec.key);
					removed++;
				}
			}
		} catch (Throwable e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		}
		if (DEBUG) {
			log("SendOwn:        " + sendOwn);
			log("SendBackup:     " + sendBackup);
			log("MigrationOwner: " + migrationOwner);
			log("OwnerBackup:    " + ownerBackup);
			log("MigrationBackup:" + migrationBackup);
			log("Removed:        " + removed);
		}
		executeLocally(new Migrator(recordsToBackup, recordsToMigrate));
	}

	private boolean backupMemberFor(Block block) {
		Address owner = block.migrationAddress;
		if (owner == null)
			owner = block.owner;
		if (owner == null)
			return false;
		MemberImpl next = getNextMemberAfter(owner, true, 1);
		return (next != null && thisAddress.equals(next.getAddress()));
	}

	private void doMigrationComplete(Address from) {
		System.out.println("Migration Compelete from " + from);
		Collection<Block> blocks = mapBlocks.values();
		for (Block block : blocks) {
			if (from.equals(block.owner)) {
				if (block.isMigrating()) {
					block.owner = block.migrationAddress;
					block.migrationAddress = null;
				} 
			}
		}
		if (isMaster() && !from.equals(thisAddress)) {
			// I am the master and I got migration complete from a member
			// I will inform others, in case they did not get it yet.
			for (MemberImpl member : lsMembers) {
				if (!member.localMember() || !from.equals(member.getAddress())) {
					Invocation inv = obtainServiceInvocation();
					inv.name = "cmap";
					inv.operation = OP_CMAP_MIGRATION_COMPLETE;
					boolean sent = send(inv, member.getAddress());
					if (!sent) {
						inv.returnToContainer();
					}
				}
			}
		}
	}

	private void sendMigrationComplete() {
		for (MemberImpl member : lsMembers) {
			if (!member.localMember()) {
				Invocation inv = obtainServiceInvocation();
				inv.name = "cmap";
				inv.operation = OP_CMAP_MIGRATION_COMPLETE;
				boolean sent = send(inv, member.getAddress());
				if (!sent) {
					inv.returnToContainer();
				}
			}
		}
	}

	private boolean sendLockBackup(Record record) {
		if (zeroBackup)
			return true;
		if (lsMembers.size() < 3)
			return true;
		Invocation inv = toInvocation(record, false);
		inv.operation = OP_CMAP_BACKUP_LOCK;
		boolean sent = send(inv, getNextMemberAfter(thisAddress, true, 1).getAddress());
		if (!sent) {
			inv.returnToContainer();
		}
		return sent;
	}

	private boolean sendBackupRemove(Record record) {
		if (zeroBackup)
			return true;
		if (lsMembers.size() < 3)
			return true;
		Invocation inv = toInvocation(record, false);
		inv.operation = OP_CMAP_BACKUP_REMOVE;
		boolean sent = send(inv, getNextMemberAfter(thisAddress, true, 1).getAddress());
		if (!sent) {
			inv.returnToContainer();
		}
		return sent;
	}

	class MBackupOp extends ResponseQueueCall {
		volatile Record record;

		public boolean backup(Record record) {
			this.record = record;
			doOp();
			return getResultAsBoolean();
		}

		public void process() {
			MemberImpl nextMember = getNextMemberAfter(thisAddress, true, 1);
			if (nextMember == null) {
				setResult(Boolean.TRUE);
			} else {
				Connection conn = ConnectionManager.get().getConnection(nextMember.getAddress());
				if (conn == null || !conn.live()) {
					setResult(OBJECT_REDO);
				} else {
					if (sendBackupAdd(record, true)) {
						setResult(Boolean.TRUE);
					} else {
						setResult(OBJECT_REDO);
					}
				}
			}
		}

		public void handleResponse(Invocation inv) {
		}
	}

	private boolean sendBackupAdd(Record record, boolean force) {
		if (zeroBackup)
			return true;
		int min = 3;
		if (force)
			min = 2;
		if (lsMembers.size() < min)
			return true;
		Invocation inv = toInvocation(record, true);
		inv.operation = OP_CMAP_BACKUP_ADD;
		boolean sent = send(inv, getNextMemberAfter(thisAddress, true, 1).getAddress());
		if (!sent) {
			inv.returnToContainer();
		}
		return sent;
	}

	private Invocation toInvocation(Record record, boolean includeValue) {
		Invocation inv = obtainServiceInvocation();
		inv.name = record.getName();
		inv.blockId = record.getBlockId();
		inv.threadId = record.lockThreadId;
		inv.lockAddress = record.lockAddress;
		inv.lockCount = record.lockCount;
		inv.doHardCopy(record.getKey(), inv.key);
		if (includeValue) {
			if (record.getValue() != null) {
				inv.doHardCopy(record.getValue(), inv.data);
			}
		}
		return inv;
	}

	boolean rightRemoteTarget(Invocation inv) {
		boolean right = thisAddress.equals(getTarget(inv.name, inv.key));
		if (!right) {
			// not the owner (at least not anymore)
			if (isMaster()) {
				Block block = getOrCreateBlock(inv.key);
				sendBlockInfo(block, inv.conn.getEndPoint());
			}
			inv.setNoData();
			sendRedoResponse(inv);
		}
		return right;
	}

	void handleMigrateRecord(Invocation inv) {
		remoteReq.setInvocation(inv);
		doMigrate(remoteReq);
		sendResponse(inv);
		remoteReq.reset();
	}

	void handleRead(Invocation inv) {
		remoteReq.setInvocation(inv);
		doRead(remoteReq);
		Record record = (Record) remoteReq.response;
		if (record != null) {
			inv.recordId = record.id;
			inv.longValue = record.getCopyCount();
			inv.doHardCopy(record.getKey(), inv.key);
			inv.doHardCopy(record.getValue(), inv.data);
		}
		sendResponse(inv);
		remoteReq.reset();
	}

	void handleAdd(Invocation inv) {
		if (rightRemoteTarget(inv)) {
			remoteReq.setInvocation(inv);
			doAdd(remoteReq);
			if (remoteReq.response == Boolean.TRUE) {
				sendResponse(inv);
			} else {
				sendResponseFailure(inv);
			}
			remoteReq.reset();
		}
	}

	final void handleGet(Invocation inv) {
		if (rightRemoteTarget(inv)) {
			remoteReq.setInvocation(inv);
			doGet(remoteReq);
			Data value = (Data) remoteReq.response;
			if (value != null && value.size() > 0) {
				inv.doHardCopy(value, inv.data);
			}
			sendResponse(inv);
			remoteReq.reset();
		}
	}

	final void handleContains(boolean containsKey, Invocation inv) {
		if (containsKey && !rightRemoteTarget(inv))
			return;
		remoteReq.setInvocation(inv);
		doContains(containsKey, remoteReq);
		if (remoteReq.response == Boolean.TRUE) {
			sendResponse(inv);
		} else {
			sendResponseFailure(inv);
		}
		remoteReq.reset();
	}

	final void handleLock(Invocation inv) {
		if (rightRemoteTarget(inv)) {
			remoteReq.setInvocation(inv);
			doLock(remoteReq);
			if (!remoteReq.scheduled) {
				if (remoteReq.response == Boolean.TRUE) {
					sendResponse(inv);
				} else {
					sendResponseFailure(inv);
				}
			} else {
				inv.returnToContainer();
			}
			remoteReq.reset();
		}
	}

	final void handlePut(Invocation inv) {
		if (rightRemoteTarget(inv)) {
			remoteReq.setInvocation(inv);
			doPut(remoteReq);
			if (!remoteReq.scheduled) {
				Data oldValue = (Data) remoteReq.response;
				if (oldValue != null && oldValue.size() > 0) {
					inv.doSet(oldValue, inv.data);
				}
				sendResponse(inv);
			} else {
				inv.returnToContainer();
			}
			remoteReq.reset();
		}
	}

	final void handleRemove(Invocation inv) {
		if (rightRemoteTarget(inv)) {
			remoteReq.setInvocation(inv);
			doRemove(remoteReq);
			if (!remoteReq.scheduled) {
				Data oldValue = (Data) remoteReq.response;
				if (oldValue != null && oldValue.size() > 0) {
					inv.doSet(oldValue, inv.data);
				}
				sendResponse(inv);
			} else {
				inv.returnToContainer();
			}
			remoteReq.reset();
		}
	}

	final void doLock(Request request) {
		boolean lock = (request.operation == OP_CMAP_LOCK) ? true : false;
		if (!lock) {
			// unlock
			boolean unlocked = true;
			Record record = recordExist(request);
			if (DEBUG) {
				log(request.operation + " unlocking " + record);
			}
			if (record != null) {
				unlocked = record.unlock(request.lockThreadId, request.lockAddress);
				if (unlocked) {
					sendLockBackup(record);
				}
			}
			if (request.local) {
				if (unlocked)
					request.response = Boolean.TRUE;
				else
					request.response = Boolean.FALSE;
			}
		} else if (!testLock(request)) {
			if (request.hasEnoughTimeToSchedule()) {
				// schedule
				final Record record = ensureRecord(request);
				final Request reqScheduled = (request.local) ? request : request.softCopy();
				if (request.operation == OP_CMAP_LOCK_RETURN_OLD) {
					reqScheduled.value = ThreadContext.get().hardCopy(record.getValue());
				}
				if (DEBUG) {
					log("scheduling lock");
				}
				record.addScheduledAction(new ScheduledLockAction(reqScheduled, record));
				request.scheduled = true;
			} else {
				request.response = Boolean.FALSE;
			}
		} else {
			if (DEBUG) {
				log("Locking...");
			}
			Record rec = ensureRecord(request);
			if (request.operation == OP_CMAP_LOCK_RETURN_OLD) {
				request.value = ThreadContext.get().hardCopy(rec.getValue());
			}
			rec.lock(request.lockThreadId, request.lockAddress);
			sendLockBackup(rec);
			request.response = Boolean.TRUE;
		}
	}

	final void doPut(Request request) {
		if (request.operation == OP_CMAP_PUT_IF_ABSENT) {
			Record record = recordExist(request);
			if (record != null && record.getValue() != null) {
				Data valueCopy = ThreadContext.get().hardCopy(record.getValue());
				request.response = valueCopy;
				return;
			}
		} else if (request.operation == OP_CMAP_REPLACE_IF_NOT_NULL) {
			Record record = recordExist(request);
			if (record == null || record.getValue() == null) {
				request.response = null;
				return;
			}
		}
		if (!testLock(request)) {
			if (request.hasEnoughTimeToSchedule()) {
				// schedule
				Record record = ensureRecord(request);
				request.scheduled = true;
				final Request reqScheduled = (request.local) ? request : request.softCopy();
				record.addScheduledAction(new ScheduledAction(request) {
					@Override
					public boolean consume() {
						CMap cmap = getMap(reqScheduled.name);
						Data oldValue = cmap.put(reqScheduled);
						reqScheduled.response = oldValue;
						reqScheduled.key = null;
						reqScheduled.value = null;
						returnScheduledAsSuccess(reqScheduled);
						return true;
					}
				});
			} else {
				request.response = null;
			}
		} else {
			CMap cmap = getMap(request.name);
			Data oldValue = cmap.put(request);
			request.response = oldValue;
		}
	}

	final void doRead(Request request) {
		CMap cmap = getMap(request.name);
		request.response = cmap.read(request);
	}

	final void doMigrate(Request request) {
		CMap cmap = getMap(request.name);
		cmap.own(request);
		request.response = Boolean.TRUE;
	}

	final void doAdd(Request request) {
		CMap cmap = getMap(request.name);
		boolean added = cmap.add(request);
		request.response = (added) ? Boolean.TRUE : Boolean.FALSE;
	}

	final void doGet(Request request) {
		CMap cmap = getMap(request.name);
		request.response = cmap.get(request);
	}

	final void doContains(boolean containsKey, Request request) {
		CMap cmap = getMap(request.name);
		if (containsKey) {
			request.response = cmap.containsKey(request);
		} else {
			request.response = cmap.containsValue(request);
		}
	}

	final void doRemove(Request request) {
		if (request.operation == OP_CMAP_REMOVE_IF_SAME) {
			Record record = recordExist(request);
			if (record == null || record.getValue() == null
					|| !record.getValue().equals(request.value)) {
				request.response = null;
				return;
			}
		}
		if (!testLock(request)) {
			if (request.hasEnoughTimeToSchedule()) {
				// schedule
				Record record = ensureRecord(request);
				request.scheduled = true;
				final Request reqScheduled = (request.local) ? request : request.softCopy();
				record.addScheduledAction(new ScheduledAction(request) {
					@Override
					public boolean consume() {
						CMap cmap = getMap(reqScheduled.name);
						Data oldValue = cmap.remove(reqScheduled);
						reqScheduled.response = oldValue;
						returnScheduledAsSuccess(reqScheduled);
						return true;
					}
				});
			} else {
				request.response = null;
			}
		} else {
			CMap cmap = getMap(request.name);
			Data oldValue = cmap.remove(request);
			request.response = oldValue;
		}
	}

	Record recordExist(Request req) {
		CMap cmap = maps.get(req.name);
		if (cmap == null)
			return null;
		Record record = cmap.getRecord(req.key);
		return record;
	}

	Record ensureRecord(Request req) {
		CMap cmap = getMap(req.name);
		Record record = cmap.getRecord(req.key);
		if (record == null) {
			record = cmap.createNewRecord(req.key, req.value);
		}
		return record;
	}

	final boolean testLock(Request req) {
		Record record = recordExist(req);
		if (record == null)
			return true;
		return record.testLock(req.lockThreadId, req.lockAddress);
	}

	final public Record getRecordById(long recordId) {
		return mapRecordsById.get(recordId);
	}

	class CMap {
		final Map<Data, Record> mapRecords = new HashMap<Data, Record>();

		final String name;

		final Map<Address, Boolean> mapListeners = new HashMap<Address, Boolean>(1);

		public CMap(String name) {
			super();
			this.name = name;
		}

		public Record getRecord(Data key) {
			return mapRecords.get(key);
		}

		public void own(Request req) {
			Record record = toRecord(req);
			record.owner = thisAddress;
			sendBackupAdd(record, true);
		}

		public void backupAdd(Request req) {
			Record record = toRecord(req);
			record.owner = req.caller;
		}

		public Record toRecord(Request req) {
			Record record = getRecord(req.key);
			if (record == null) {
				record = createNewRecord(req.key, req.value);
			} else {
				if (req.value != null) {
					record.setValue(req.value);
				}
			}
			record.lockAddress = req.lockAddress;
			record.lockThreadId = req.lockThreadId;
			record.lockCount = req.lockCount;
			return record;
		}

		public void backupRemove(Request req) {
			removeRecord(req.key);
		}

		public void backupLock(Request req) {
			toRecord(req);
		}

		public int backupSize() {
			int size = 0;
			Collection<Record> records = mapRecords.values();
			for (Record record : records) {
				if (!record.owner.equals(thisAddress)) {
					size++;
				}
			}
			return size;
		}

		public int size() {
			int size = 0;
			Collection<Record> records = mapRecords.values();
			for (Record record : records) {
				if (record.value != null) {
					Block block = mapBlocks.get(record.blockId);
					if (thisAddress.equals(block.owner)) {
						size++;
						size += record.getCopyCount();
					}
				}
			}
			return size;
		}

		public boolean containsKey(Request req) {
			Record record = getRecord(req.key);
			if (record == null)
				return false;
			else {
				return (record.getValue() != null);
			}
		}

		public boolean containsValue(Request req) {
			Data value = req.value;
			Collection<Record> records = mapRecords.values();
			for (Record record : records) {
				if (value.equals(record.getValue()))
					return true;
			}
			return false;
		}

		public Data get(Request req) {
			Record record = getRecord(req.key);
			if (record == null)
				return null;
			return record.getValue();
		}

		public boolean add(Request req) {
			Record record = getRecord(req.key);
			if (record == null) {
				req.value = ThreadContext.get().hardCopy(req.key);
				record = createNewRecord(req.key, req.value);
			} else {
				if (req.operation == OP_CMAP_ADD_TO_LIST) {
					record.incrementCopyAndGet();
				} else {
					return false;
				}
			}
			return true;
		}

		public Data put(Request req) {
			Record record = getRecord(req.key);
			Data oldValue = null;
			if (record == null) {
				record = createNewRecord(req.key, req.value);
			} else {
				oldValue = record.getValue();
				record.setValue(req.value);
			}
			if (oldValue == null) {
				fireMapEvent(mapListeners, name, EntryEvent.TYPE_ADDED, record, null);
			} else {
				fireMapEvent(mapListeners, name, EntryEvent.TYPE_UPDATED, record, null);
			}
			sendBackupAdd(record, false);
			return oldValue;
		}

		public Data remove(Request req) {
			Record record = mapRecords.get(req.key);
			if (record == null) {
				return null;
			}
			if (record.getCopyCount() > 0) {
				record.decrementCopyAndGet();
			} else {
				record = removeRecord(req.key);
			}
			if (record.getValue() != null) {
				fireMapEvent(mapListeners, name, EntryEvent.TYPE_REMOVED, record, null);
			}
			sendBackupRemove(record);
			return record.getValue();
		}

		Record removeRecord(Data key) {
			Record rec = mapRecords.remove(key);
			if (rec != null) {
				mapRecordsById.remove(rec.id);
			}
			return rec;
		}

		public Record read(Request req) {
			return nextOwnedRecord(req.recordId, req.blockId);

		}

		Record nextOwnedRecord(long recordId, int blockId) {
			Record rec = null;
			while (rec == null && recordId <= maxId) {
				rec = getRecordById(recordId);
				if (rec != null) {
					if (rec.blockId == blockId) {
						if (rec.owner.equals(thisAddress)) {
							return rec;
						}
					}
				}
				rec = null;
				recordId++;
			}
			return rec;
		}

		Record createNewRecord(Data key, Data value) {
			long id = ++maxId;
			int blockId = getBlockId(key);
			Record rec = new Record(id, name, blockId, key, value);
			rec.owner = getOrCreateBlock(key).owner;
			mapRecords.put(key, rec);
			mapRecordsById.put(rec.id, rec);
			// if (DEBUG) log("createNewRecord size " + mapRecords.size());
			return rec;
		}

		public long maxRecordId() {
			return maxId;
		}

		public void addListener(Data key, Address address, boolean includeValue) {
			if (key == null || key.size() == 0) {
				mapListeners.put(address, includeValue);
			} else {
				Record rec = getRecord(key);
				if (rec == null) {
					rec = createNewRecord(key, null);
				}
				rec.addListener(address, includeValue);
			}
		}

		public void removeListener(Data key, Address address) {
			if (key == null || key.size() == 0) {
				mapListeners.remove(address);
			} else {
				Record rec = getRecord(key);
				if (rec != null) {
					rec.removeListener(address);
				}
			}
		}

		@Override
		public String toString() {
			return "CMap [" + name + "] size=" + size() + ", backup-size=" + backupSize();
		}
	}

	class Record {

		private Address owner;
		private Data key;
		private Data value;
		private int version = 0;
		private final long createTime;
		private final long id;
		private final String name;
		private final int blockId;
		private int lockThreadId = -1;
		private Address lockAddress = null;
		private int lockCount = 0;
		private List<ScheduledAction> lsScheduledActions = null;
		private Map<Address, Boolean> mapListeners = null;
		private AtomicInteger copyCount = null;
		private List<Data> lsValues = null;

		public Record(long id, String name, int blockId, Data key, Data value) {
			super();
			this.name = name;
			this.blockId = blockId;
			this.id = id;
			this.key = key;
			this.value = value;
			this.createTime = System.currentTimeMillis();
		}

		public long getId() {
			return id;
		}

		public int getBlockId() {
			return blockId;
		}

		public String getName() {
			return name;
		}

		public Address getOwner() {
			return owner;
		}

		public void setOwner(Address owner) {
			this.owner = owner;
		}

		public Data getKey() {
			return key;
		}

		public void setKey(Data key) {
			this.key = key;
		}

		public Data getValue() {
			return value;
		}

		public void setValue(Data value) {
			this.value = value;
			version++;
		}

		public void addValue(Data value) {
			if (this.value == null) {
				this.value = value;
			} else {
				if (lsValues == null) {
					lsValues = new ArrayList<Data>(5);
				}
				lsValues.add(value);
			}
		}

		public void onDisconnect(Address deadAddress) {
			if (lsScheduledActions != null) {
				if (lsScheduledActions.size() > 0) {
					Iterator<ScheduledAction> it = lsScheduledActions.iterator();
					while (it.hasNext()) {
						ScheduledAction sa = it.next();
						if (sa.request.caller.equals(deadAddress)) {
							ClusterManager.get().deregisterScheduledAction(sa);
							sa.setValid(false);
							it.remove();
						}
					}
				}
			}
			if (lockCount > 0) {
				if (deadAddress.equals(lockAddress)) {
					unlock();
				}
			}
		}

		public void unlock() {
			lockThreadId = -1;
			lockCount = 0;
			lockAddress = null;
			fireScheduledActions();
		}

		public boolean unlock(int threadId, Address address) {
			if (threadId == -1 || address == null)
				throw new IllegalArgumentException();
			if (lockCount == 0)
				return true;
			if (lockThreadId != threadId || !address.equals(lockAddress)) {
				return false;
			}
			if (lockCount > 0) {
				lockCount--;
			}
			if (DEBUG) {
				log(lsScheduledActions + " unlocked " + lockCount);
			}
			if (lockCount == 0) {
				lockThreadId = -1;
				lockAddress = null;
				fireScheduledActions();
			}
			return true;
		}

		public void fireScheduledActions() {
			if (lsScheduledActions != null) {
				while (lsScheduledActions.size() > 0) {
					ScheduledAction sa = lsScheduledActions.remove(0);
					ClusterManager.get().deregisterScheduledAction(sa);
					if (DEBUG) {
						log("sa.expired " + sa.expired());
					}
					if (sa.expired()) {
						sa.onExpire();
					} else {
						sa.consume();
						return;
					}
				}
			}
		}

		public boolean testLock(int threadId, Address address) {
			if (lockCount == 0) {
				return true;
			}
			if (lockThreadId == threadId && lockAddress.equals(address)) {
				return true;
			}
			return false;
		}

		public boolean lock(int threadId, Address address) {
			if (lockCount == 0) {
				lockThreadId = threadId;
				lockAddress = address;
				lockCount++;
				return true;
			}
			if (lockThreadId == threadId && lockAddress.equals(address)) {
				lockCount++;
				return true;
			}
			return false;
		}

		public void addScheduledAction(ScheduledAction scheduledAction) {
			if (lsScheduledActions == null)
				lsScheduledActions = new ArrayList<ScheduledAction>(1);
			lsScheduledActions.add(scheduledAction);
			ClusterManager.get().registerScheduledAction(scheduledAction);
			if (DEBUG) {
				log("scheduling " + scheduledAction);
			}
		}

		public int scheduledActionCount() {
			if (lsScheduledActions == null)
				return 0;
			else
				return lsScheduledActions.size();
		}

		public int getVersion() {
			return version;
		}

		public void setVersion(int version) {
			this.version = version;
		}

		public long getCreateTime() {
			return createTime;
		}

		public boolean hasListener() {
			return (mapListeners != null && mapListeners.size() > 0);
		}

		public Map<Address, Boolean> getMapListeners() {
			return mapListeners;
		}

		public void addListener(Address address, boolean returnValue) {
			if (mapListeners == null)
				mapListeners = new HashMap<Address, Boolean>(1);
			mapListeners.put(address, returnValue);
		}

		public void removeListener(Address address) {
			if (mapListeners == null)
				return;
			mapListeners.remove(address);
		}

		public int incrementCopyAndGet() {
			if (copyCount == null)
				copyCount = new AtomicInteger(1);
			return copyCount.incrementAndGet();
		}

		public int decrementCopyAndGet() {
			if (copyCount == null)
				return 0;
			return copyCount.decrementAndGet();
		}

		public int getCopyCount() {
			if (copyCount == null)
				return 0;
			return copyCount.get();
		}

		public String toString() {
			return "Record [" + id + "] key=" + key;
		}
	}

	public static class Block implements DataSerializable {
		int blockId;
		Address owner;
		Address migrationAddress;

		public Block() {
		}

		public Block(int blockId, Address owner) {
			this.blockId = blockId;
			this.owner = owner;
		}

		public Block(int blockId, Address owner, Address migrationAddress) {
			this.blockId = blockId;
			this.owner = owner;
			this.migrationAddress = migrationAddress;
		}

		public Address getMigrationAddress() {
			return migrationAddress;
		}

		public void setMigrationAddress(Address migrationAddress) {
			this.migrationAddress = migrationAddress;
		}

		public boolean isMigrating() {
			return (migrationAddress != null);
		}

		public void readData(DataInput in) throws IOException {
			blockId = in.readInt();
			boolean owned = in.readBoolean();
			if (owned) {
				owner = new Address();
				owner.readData(in);
			}
			boolean migrating = in.readBoolean();
			if (migrating) {
				migrationAddress = new Address();
				migrationAddress.readData(in);
			}
		}

		public void writeData(DataOutput out) throws IOException {
			out.writeInt(blockId);
			boolean owned = (owner != null);
			out.writeBoolean(owned);
			if (owned) {
				owner.writeData(out);
			}
			boolean migrating = (migrationAddress != null);
			out.writeBoolean(migrating);
			if (migrating)
				migrationAddress.writeData(out);
		}

		@Override
		public String toString() {
			return "Block [" + blockId + "] owner=" + owner + " migrationAddress="
					+ migrationAddress;
		}
	}

	public static class Blocks extends AbstractRemotelyProcessable {
		List<Block> lsBlocks = new ArrayList<Block>(BLOCK_COUNT);

		public void addBlock(Block block) {
			lsBlocks.add(block);
		}

		public void readData(DataInput in) throws IOException {
			int size = in.readInt();
			for (int i = 0; i < size; i++) {
				Block block = new Block();
				block.readData(in);
				addBlock(block);
			}
		}

		public void writeData(DataOutput out) throws IOException {
			out.writeInt(lsBlocks.size());
			for (Block block : lsBlocks) {
				block.writeData(out);
			}
		}

		public void process() {
			ConcurrentMapManager.get().handleBlocks(Blocks.this);
		}
	}

}
