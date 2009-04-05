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

import static com.hazelcast.impl.Constants.ClusterOperations.OP_REMOTELY_PROCESS;
import static com.hazelcast.impl.Constants.ClusterOperations.OP_RESPONSE;
import static com.hazelcast.impl.Constants.EventOperations.OP_EVENT;
import static com.hazelcast.impl.Constants.MapTypes.MAP_TYPE_LIST;
import static com.hazelcast.impl.Constants.MapTypes.MAP_TYPE_MAP;
import static com.hazelcast.impl.Constants.MapTypes.MAP_TYPE_MULTI_MAP;
import static com.hazelcast.impl.Constants.MapTypes.MAP_TYPE_SET;
import static com.hazelcast.impl.Constants.Objects.OBJECT_NULL;
import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import static com.hazelcast.impl.Constants.ResponseTypes.RESPONSE_FAILURE;
import static com.hazelcast.impl.Constants.ResponseTypes.RESPONSE_NONE;
import static com.hazelcast.impl.Constants.ResponseTypes.RESPONSE_REDO;
import static com.hazelcast.impl.Constants.ResponseTypes.RESPONSE_SUCCESS;
import static com.hazelcast.nio.BufferUtil.*;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IMap;
import com.hazelcast.impl.ClusterManager.RemotelyProcessable;
import com.hazelcast.impl.ConcurrentMapManager.Record;
import com.hazelcast.nio.PacketQueue;
import com.hazelcast.nio.*;
import com.hazelcast.nio.PacketQueue.Packet;

abstract class BaseManager implements Constants {

	protected final static boolean zeroBackup = false;

    private final static int EVENT_QUEUE_COUNT = 100;

	protected static Logger logger = Logger.getLogger(BaseManager.class.getName());

	protected final static LinkedList<MemberImpl> lsMembers = new LinkedList<MemberImpl>();

	protected final static Map<Address, MemberImpl> mapMembers = new HashMap<Address, MemberImpl>(
			100);

	protected final static boolean DEBUG = Build.DEBUG;

	protected final static Map<Long, Call> mapCalls = new HashMap<Long, Call>();

	protected final static Map<String, OrderedEventQueue> mapOrderedEventQueues = new HashMap<String, OrderedEventQueue>(
			10);

	protected final static EventQueue[] eventQueues = new EventQueue[EVENT_QUEUE_COUNT];

	protected final static Map<Long, StreamResponseHandler> mapStreams = new ConcurrentHashMap<Long, StreamResponseHandler>();

	private static long scheduledActionIdIndex = 0;

	private static long callId = 1;

	private static long idGen = 0;
	
	protected final Address thisAddress;

	protected final MemberImpl thisMember;


    protected BaseManager() {
		thisAddress = Node.get().address;
		thisMember = Node.get().localMember;
		for (int i = 0; i < EVENT_QUEUE_COUNT; i++) {
			eventQueues[i] = new EventQueue();
		}
	}

    public abstract class ScheduledAction {
		protected long timeToExpire;

		protected long timeout;

		protected boolean valid = true;

		protected Request request = null;

		protected final long id;

		public ScheduledAction(final Request request) {
			this.request = request;
			setTimeout(request.timeout);
			id = scheduledActionIdIndex++;
		}

		public abstract boolean consume();

		@Override
		public boolean equals(final Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final ScheduledAction other = (ScheduledAction) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (id != other.id)
				return false;
			return true;
		}

		public boolean expired() {
			if (!valid)
				return true;
			if (timeout == -1)
				return false;
			else
				return System.currentTimeMillis() >= getExpireTime();
		}

		public long getExpireTime() {
			return timeToExpire;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + (int) (id ^ (id >>> 32));
			return result;
		}

		public boolean isValid() {
			return valid;
		}

		public boolean neverExpires() {
			return (timeout == -1);
		}

		public void onExpire() {

		}

		public void setTimeout(long timeout) {
			if (timeout > -1) {
				this.timeout = timeout;				
				timeToExpire = System.currentTimeMillis() + timeout;
			} else {
				this.timeout = -1;
			}
		}

		public void setValid(final boolean valid) {
			this.valid = valid;
		}

		@Override
		public String toString() {
			return "ScheduledAction {neverExpires=" + neverExpires() + ", timeout= " + timeout
					+ "}";
		}

		private BaseManager getOuterType() {
			return BaseManager.this;
		}
	}

	public static class SimpleDataEntry implements Map.Entry {
		String name;

		int blockId;

		Data keyData;

		Data valueData;

		Object key = null;

		Object value = null;

		int copyCount = 0;

		public SimpleDataEntry(final String name, final int blockId, final Data key,
				final Data value, final int copyCount) {
			super();
			this.blockId = blockId;
			this.keyData = key;
			this.name = name;
			this.valueData = value;
			this.copyCount = copyCount - 1;
		}
		
		public SimpleDataEntry (final String name, final Object key, final Object value) {
			this.key = key;
			this.value = value;
			this.name = name;
		}

		public int getBlockId() {
			return blockId;
		}

		public Object getKey() {
			if (key == null) {
				key = ThreadContext.get().toObject(keyData);
			}
			return key;
		}

		public Data getKeyData() {
			return keyData;
		}

		public String getName() {
			return name;
		}

		public Object getValue() {
			if (value == null) {
				if (valueData != null) {
					value = ThreadContext.get().toObject(valueData);
				} else {
                    return getKey();
                }
			}
			return value;
		}

		public Data getValueData() {
			return valueData;
		}

		public Object setValue(final Object value) {
			final IMap map = (IMap) FactoryImpl.getProxy(name);
			map.put(keyData, value);
			final Object oldValue = value;
			this.value = value;
			return oldValue;
		}

		@Override
		public String toString() {
			return "MapEntry key=" + getKey() + ", value=" + getValue();
		}
	}

	abstract class AbstractCall implements Call {
		private long id = -1;

		public AbstractCall() {
		}

		public long getId() {
			return id;
		}

		public void onDisconnect(final Address dead) {
		}

		public void redo() {
			removeCall(getId());
			id = -1;
			enqueueAndReturn(this);
		}

		public void setId(final long id) {
			this.id = id;
		}
	}

	abstract class AllOp extends RequestBasedCall {
		int numberOfExpectedResponses = 0;

		int numberOfResponses = 0;

		protected boolean done = false;

		protected Set<Address> setAddresses = new HashSet<Address>();

		public AllOp() {
		}

		@Override
		public Object getResult() {
			return null;
		}

		public void handleResponse(final PacketQueue.Packet packet) {
			consumeResponse(packet);
		}

		@Override
		public void onDisconnect(final Address dead) {
			log(dead + " onDisconnect " + AllOp.this);
			reset();
			redo();
		}

		public void process() {
			doLocalOp();
			if (setAddresses.size() == 0) {
				for (final MemberImpl member : lsMembers) {
					setAddresses.add(member.getAddress());
				}
			}
			numberOfResponses = 1;
			numberOfExpectedResponses = setAddresses.size();
			if (setAddresses.size() > 1) {
				addCall(AllOp.this);
				for (final Address address : setAddresses) {
					if (!address.equals(thisAddress)) {
						final PacketQueue.Packet packet = request.toPacket();
						packet.callId = getId();
						final boolean sent = send(packet, address);
						if (!sent) {
							packet.returnToContainer();
							log(address + " not reachable: operation redoing:  " + AllOp.this);
							redo();
						}

					}
				}
			} else {
				complete(false);
			}
		}

		public void reset() {
			done = false;
			numberOfResponses = 0;
			numberOfExpectedResponses = 0;
			setAddresses.clear();
		}

		void complete(final boolean call) {
			if (!done) {
				done = true;
				if (call) {
					removeCall(getId());
				}
				synchronized (this) {
					notify();
				}
			}
		}

		void consumeResponse(final PacketQueue.Packet packet) {
			numberOfResponses++;
			if (numberOfResponses >= numberOfExpectedResponses) {
				complete(true);
			}
			packet.returnToContainer();
		}

		abstract void doLocalOp();

		@Override
		void doOp() {
			reset();
			try {
				synchronized (this) {
					enqueueAndReturn(this);
					wait();
				}
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}
	}

	abstract class BooleanOp extends TargetAwareOp {
		@Override
		void handleNoneRedoResponse(final PacketQueue.Packet packet) {
			handleBooleanNoneRedoResponse(packet);
		}
	}

	interface Call extends Processable {

		long getId();

		void handleResponse(PacketQueue.Packet packet);

		void onDisconnect(Address dead);

		void setId(long id);
	}

	abstract class LongOp extends TargetAwareOp {
		@Override
		void handleNoneRedoResponse(final PacketQueue.Packet packet) {
			handleLongNoneRedoResponse(packet);
		}
	}

	static class OrderedEventQueue extends ConcurrentLinkedQueue<EventTask> implements Runnable {
		protected static Logger logger = Logger.getLogger(OrderedEventQueue.class.getName());

		volatile long expectedRecordId = -1;

		Map<Long, EventTask> mapDelayedEventTasks = new HashMap<Long, EventTask>(10);

		public OrderedEventQueue(final long expectedRecordId) {
			super();
			this.expectedRecordId = expectedRecordId;
		}

		public void run() {
			EventTask eventTask = poll();
			if (eventTask == null)
				return;
			logger.log(Level.FINEST, expectedRecordId + "  running event " + eventTask.recordId);
			if (expectedRecordId == eventTask.recordId) {
				try {
					eventTask.run();
				} catch (final Throwable e) {
					e.printStackTrace();
				}
				expectedRecordId++;
				while (eventTask != null && mapDelayedEventTasks.size() > 0) {
					eventTask = mapDelayedEventTasks.remove(expectedRecordId);
					if (eventTask != null) {
						eventTask.run();
						expectedRecordId++;
					}
				}
			} else if (eventTask.recordId > expectedRecordId) {
				mapDelayedEventTasks.put(eventTask.recordId, eventTask);
				// ignore recordIds less than expected
			}
		}
	}

	interface Processable {
		void process();
	}

	abstract class QueueBasedCall extends AbstractCall {
		final protected BlockingQueue responses;

		public QueueBasedCall() {
			this(true);
		}

		public QueueBasedCall(final boolean limited) {
			if (limited) {
				responses = new ArrayBlockingQueue(1);
			} else {
				responses = new LinkedBlockingQueue();
			}
		}

		public void handleBooleanNoneRedoResponse(final PacketQueue.Packet packet) {
			removeCall(getId());
			if (packet.responseType == ResponseTypes.RESPONSE_SUCCESS) {
				responses.add(Boolean.TRUE);
			} else {
				responses.add(Boolean.FALSE);
			}
		}

		@Override
		public void redo() {
			removeCall(getId());
			responses.clear();
			responses.add(OBJECT_REDO);
		}

		void handleObjectNoneRedoResponse(final PacketQueue.Packet packet) {
			removeCall(getId());
			if (packet.responseType == ResponseTypes.RESPONSE_SUCCESS) {
				final Data oldValue = doTake(packet.value);
				if (oldValue == null || oldValue.size() == 0) {
					responses.add(OBJECT_NULL);
				} else {
					responses.add(oldValue);
				}
			} else {
				throw new RuntimeException("responseType " + packet.responseType);
			}
		}
	}

	class Request {
		volatile int redoCount = 0;

		volatile int resetCount = 0;

		boolean local = true;

		int operation = -1;

		String name = null;

		Data key = null;

		Data value = null;

		int blockId = -1;

		long timeout = -1;

		long txnId = -1;

		Address caller = null;

		int lockThreadId = -1;

		Address lockAddress = null;

		int lockCount = 0;

		long eventId = -1;

		long longValue = -1;

		long recordId = -1;
		
		long version = -1;

		Object attachment = null;

		Object response = null;

		boolean scheduled = false;

		public boolean hasEnoughTimeToSchedule() {
			return timeout == -1 || timeout > 100;
		}

		public void reset() {
			if (this.key != null) {
				this.key.setNoData();
			}
			if (this.value != null) {
				this.value.setNoData();
			}	
			this.resetCount++;
			this.local = true;
			this.operation = -1;
			this.name = null;
			this.key = null;
			this.value = null;
			this.blockId = -1;
			this.timeout = -1;
			this.txnId = -1;
			this.eventId = -1;
			this.lockThreadId = -1;
			this.lockAddress = null;
			this.lockCount = 0;
			this.caller = null;
			this.longValue = -1;
			this.response = null;
			this.scheduled = false;
			this.attachment = null;
			this.recordId = -1;
			this.version = -1;
		}

		public void set(final boolean local, final int operation, final String name,
				final Data key, final Data value, final int blockId, final long timeout,
				final long txnId, final long eventId, final int lockThreadId,
				final Address lockAddress, final int lockCount, final Address caller,
				final long longValue, final long recordId, final long version) {					
			this.local = local;
			this.operation = operation;
			this.name = name;
			this.key = key;
			this.value = value;
			this.blockId = blockId;
			this.timeout = timeout;
			this.txnId = txnId;
			this.eventId = eventId;
			this.lockThreadId = lockThreadId;
			this.lockAddress = lockAddress;
			this.lockCount = lockCount;
			this.caller = caller;
			this.longValue = longValue;
			this.recordId = recordId;
			this.version = version;
		}

		public void setPacket(final PacketQueue.Packet packet) {
			reset();
			set(false, packet.operation, packet.name, doTake(packet.key), doTake(packet.value),
					packet.blockId, packet.timeout, packet.txnId, packet.callId, packet.threadId,
					packet.lockAddress, packet.lockCount, packet.conn.getEndPoint(), packet.longValue,
					packet.recordId, packet.version);

		}

		public void setLocal(final int operation, final String name, final Data key,
				final Data value, final int blockId, final long timeout, final long recordId) {
			reset();
			set(true, operation, name, key, value, blockId, timeout, -1, -1, -1, thisAddress, 0,
					thisAddress, -1, recordId, -1);
			this.txnId = ThreadContext.get().getTxnId();
			this.lockThreadId = Thread.currentThread().hashCode();
			this.caller = thisAddress;
		}

		public Request hardCopy() {
			final Request copy = new Request();
			Data newKey = doHardCopy(key);
			Data newValue = doHardCopy(value);
			
			copy.set(local, operation, name, newKey, newValue, blockId, timeout, txnId, eventId,
					lockThreadId, lockAddress, lockCount, caller, longValue, recordId, version);
			copy.attachment = attachment;
			copy.response = response;
			copy.scheduled = scheduled;
			return copy;
		}

		public PacketQueue.Packet toPacket() {
			final PacketQueue.Packet packet = obtainPacket();
			packet.local = false;
			packet.operation = operation;
			packet.name = name;
			if (key != null)
				doHardCopy(key, packet.key);
			if (value != null)
				doHardCopy(value, packet.value);
			packet.blockId = blockId;
			packet.timeout = timeout;
			packet.txnId = txnId;
			packet.callId = eventId;
			packet.threadId = lockThreadId;
			packet.lockAddress = lockAddress;
			packet.lockCount = lockCount;
			packet.longValue = longValue;
			packet.recordId = recordId;
			packet.version = version;
			return packet;
		}
    }

	abstract class RequestBasedCall extends AbstractCall {
		final protected Request request = new Request();

		public boolean booleanCall(final int operation, final String name, final Object key,
				final Object value, final long timeout, final long txnId, final long recordId) {
			doOp(operation, name, key, value, timeout, txnId, recordId);
			return getResultAsBoolean();
		}

		public void doOp(final int operation, final String name, final Object key,
				final Object value, final long timeout, final long txnId, final long recordId) {
			setLocal(operation, name, key, value, timeout, txnId, recordId);
			doOp();
		}

		public boolean getResultAsBoolean() {
			try {
				final Object result = getResult();
				if (result == OBJECT_NULL || result == null) {
					return false;
				}
				if (result == Boolean.TRUE)
					return true;
				else
					return false;
			} catch (final Exception e) {
				logger.log(Level.SEVERE, "getResultAsBoolean", e);
			} finally { 
				request.reset();
			}
			return false;
		}
		 

		public Object getResultAsObject() {
			try {
				final Object result = getResult(); 
				
				if (result == OBJECT_NULL || result == null) {
					return null;
				}
				if (result instanceof Data) {
					final Data data = (Data) result;
					if (data.size() == 0)
						return null;   
					return ThreadContext.get().toObject(data);
				}
				return result;
			} catch (final Throwable e) {
				logger.log(Level.SEVERE, "getResultAsObject", e);
			} finally { 
				request.reset();
			}
			return null;
		}

		public Object objectCall() {
			doOp();
			return getResultAsObject();
		}

		public Object objectCall(final int operation, final String name, final Object key,
				final Object value, final long timeout, final long txnId, final long recordId) {
			setLocal(operation, name, key, value, timeout, txnId, recordId);
			return objectCall();
		}

		public void setLocal(final int operation, final String name, final Object key,
				final Object value, final long timeout, final long txnId, final long recordId) {
			Data keyData = null;
			Data valueData = null;
			if (key != null) {
				keyData = ThreadContext.get().toData(key);
			}
			if (value != null) {
				valueData = ThreadContext.get().toData(value);
			}
			request.setLocal(operation, name, keyData, valueData, -1, timeout, recordId);
			request.attachment = this;
		}

		abstract void doOp();

		abstract Object getResult();

	}

	abstract class ResponseQueueCall extends RequestBasedCall {
		final protected BlockingQueue responses;

		public ResponseQueueCall() {
			this(true);
		}

		public ResponseQueueCall(final boolean limited) {
			if (limited) {
				responses = new ArrayBlockingQueue(1);
			} else {
				responses = new LinkedBlockingQueue();
			}
		}

		@Override
		public void doOp() {
			responses.clear();
			enqueueAndReturn(ResponseQueueCall.this);
		}

		@Override
		public Object getResult() {
			Object result = null;
			try {
				result = responses.take();
				if (result == OBJECT_REDO) {
					Thread.sleep(2000);
					// if (DEBUG) {
					// log(getId() + " Redoing.. " + this);
					// }
					request.redoCount++;
					doOp();
					return getResult();
				}
			} catch (final Exception e) {
				e.printStackTrace(System.out);
			}
			return result;
		}

		@Override
		public void redo() {
			removeCall(getId());
			responses.clear();
			setResult(OBJECT_REDO);
		}
		
		public void handleBooleanNoneRedoResponse(final PacketQueue.Packet packet) {
			removeCall(getId());
			if (packet.responseType == ResponseTypes.RESPONSE_SUCCESS) {
				setResult(Boolean.TRUE);
			} else {
				setResult(Boolean.FALSE);
			}
		}

		void handleLongNoneRedoResponse(final PacketQueue.Packet packet) {
			removeCall(getId());
			if (packet.responseType == ResponseTypes.RESPONSE_SUCCESS) {
				setResult(packet.longValue);
			} else {
				throw new RuntimeException("handleLongNoneRedoResponse.responseType "
						+ packet.responseType);
			}
		}

		void handleObjectNoneRedoResponse(final PacketQueue.Packet packet) {
			removeCall(getId());
			if (packet.responseType == ResponseTypes.RESPONSE_SUCCESS) {
				final Data oldValue = doTake(packet.value);
				if (oldValue == null || oldValue.size() == 0) {
					setResult(OBJECT_NULL);
				} else {
					setResult(oldValue);
				}
			} else {
				throw new RuntimeException("handleObjectNoneRedoResponse.responseType "
						+ packet.responseType);
			}
		}

		void setResult(final Object obj) { 
			if (obj == null) {
				responses.add(OBJECT_NULL);
			} else {
				responses.add(obj);
			}
		}
	}

	class CheckAllConnectionsOp extends ResponseQueueCall {

		public boolean check() {
			doOp();
			return getResultAsBoolean();
		}

		public void process() {
			for (MemberImpl member : lsMembers) {
				if (!member.localMember()) {
					Connection conn = ConnectionManager.get().getConnection(member.getAddress());
					if (conn == null || !conn.live()) {
						setResult(OBJECT_REDO);
						return;
					}
				}
			}
			setResult(Boolean.TRUE);
		}

		public void handleResponse(PacketQueue.Packet packet) {
		}
	}

	abstract class TargetAwareOp extends ResponseQueueCall {

		Address target = null;

		public TargetAwareOp() {
		}

		public void handleResponse(final PacketQueue.Packet packet) {
			if (packet.responseType == RESPONSE_REDO) {
				redo();
			} else {
				handleNoneRedoResponse(packet);
			}
			packet.returnToContainer();
		}

		@Override
		public void onDisconnect(final Address dead) {
			if (dead.equals(target)) {
				redo();
			}
		}

		public void process() {
			setTarget();
			if (target == null) {
				setResult(OBJECT_REDO);
				return;
			}
			if (target.equals(thisAddress)) {
				doLocalOp();
			} else {
				invoke();
			}
		}

		protected void postProcess() {

		}

		protected void invoke () {
			addCall(TargetAwareOp.this);
			final PacketQueue.Packet packet = request.toPacket();
			packet.callId = getId();
			final boolean sent = send(packet, target);
			if (!sent) {
				if (DEBUG) {
					log("packetocation cannot be sent to " + target);
				}
				packet.returnToContainer();
				redo();
			}
		}

		abstract void doLocalOp();

		void handleNoneRedoResponse(final PacketQueue.Packet packet) {
			handleObjectNoneRedoResponse(packet);
		}

		abstract void setTarget();
	}

	public static byte getMapType(final String name) {
		byte mapType = MAP_TYPE_MAP;
		if (name.length() > 3) {
			final String typeStr = name.substring(2, 4);
			if ("s:".equals(typeStr))
				mapType = MAP_TYPE_SET;
			else if ("l:".equals(typeStr))
				mapType = MAP_TYPE_LIST;
			else if ("u:".equals(typeStr))
				mapType = MAP_TYPE_MULTI_MAP;
		}
		return mapType;
	}

	public long addCall(final Call call) {
		final long id = idGen++;
		call.setId(id); 
		mapCalls.put(id, call);
		return id;
	}

	public void enqueueAndReturn(final Object obj) {
		ClusterService.get().enqueueAndReturn(obj);
	}

	public Address getKeyOwner(final Data key) {
		return ConcurrentMapManager.get().getKeyOwner(null, key);
	}

	public MemberImpl getLocalMember() {
		return ClusterManager.get().getLocalMember();
	}

	public PacketQueue.Packet obtainPacket(final String name, final Object key,
			final Object value, final int operation, final long timeout) {
		try {
			final PacketQueue.Packet packet = obtainPacket();
			packet.set(name, operation, key, value);
			packet.timeout = timeout;
			return packet;

		} catch (final Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public Call removeCall(final Long id) {
		return mapCalls.remove(id);
	}

	public void returnScheduledAsBoolean(final Request request) {
		if (request.local) {
			final TargetAwareOp mop = (TargetAwareOp) request.attachment;
			mop.setResult(request.response);
		} else {
			final PacketQueue.Packet packet = request.toPacket();
			if (request.response == Boolean.TRUE) {
				final boolean sent = sendResponse(packet, request.caller);
				if (DEBUG) {
					log(request.local + " returning scheduled response " + sent);
				}
			} else {
				sendResponseFailure(packet, request.caller);
			}
		}
	}

	public void returnScheduledAsSuccess(final Request request) {
		if (request.local) {
			final TargetAwareOp mop = (TargetAwareOp) request.attachment;
			mop.setResult(request.response);
		} else {
			final PacketQueue.Packet packet = request.toPacket();
			final Object result = request.response;
			if (result != null) {
				if (result instanceof Data) {
					final Data data = (Data) result;
					if (data.size() > 0) {
						doSet(data, packet.value);
					}
				}
			}
			sendResponse(packet, request.caller);
		}
	}

	public void sendEvents(final int eventType, final String name, final Data key,
			final Data value, final Map<Address, Boolean> mapListeners, final long recordId) {
		if (mapListeners != null) {
			final PacketQueue sq = PacketQueue.get();
			final Set<Map.Entry<Address, Boolean>> entries = mapListeners.entrySet();

			for (final Map.Entry<Address, Boolean> entry : entries) {
				final Address address = entry.getKey();
				final boolean includeValue = entry.getValue();
				if (address.isThisAddress()) {
					try {
						final Data eventKey = (key != null) ? ThreadContext.get().hardCopy(key)
								: null;
						Data eventValue = null;
						if (includeValue)
							eventValue = ThreadContext.get().hardCopy(value);
						enqueueEvent(eventType, name, eventKey, eventValue, address, recordId);
					} catch (final Exception e) {
						e.printStackTrace();
					}
				} else {
					final PacketQueue.Packet packet = sq.obtainPacket();
					packet.reset();
					try {
						final Data eventKey = key;
						Data eventValue = null;
						if (includeValue)
							eventValue = value;
						packet.set(name, OP_EVENT, eventKey, eventValue);
						packet.longValue = eventType;
						packet.recordId = recordId;
					} catch (final Exception e) {
						e.printStackTrace();
					}
					final boolean sent = send(packet, address);
					if (!sent)
						packet.returnToContainer();
				}
			}
		}
	}

	public void sendProcessableTo(final RemotelyProcessable rp, final Address address) {
		final Data value = ThreadContext.get().toData(rp);
		final PacketQueue.Packet packet = obtainPacket();
		try {
			packet.set("remotelyProcess", OP_REMOTELY_PROCESS, null, value);
			final boolean sent = send(packet, address);
			if (!sent) {
				packet.returnToContainer();
			}
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

	protected final void executeLocally(final Runnable runnable) {
		ExecutorManager.get().executeLocaly(runnable);
	}

	protected Address getMasterAddress() {
		return Node.get().getMasterAddress();
	}

	protected final MemberImpl getNextMemberAfter(final Address address,
			final boolean skipSuperClient, final int distance) {
		return getNextMemberAfter(lsMembers, address, skipSuperClient, distance);
	}

	protected final MemberImpl getNextMemberAfter(final List<MemberImpl> lsMembers,
			final Address address, final boolean skipSuperClient, final int distance) {
		final int size = lsMembers.size();
		if (size <= 1)
			return null;
		int indexOfMember = -1;
		for (int i = 0; i < size; i++) {
			final MemberImpl member = lsMembers.get(i);
			if (member.getAddress().equals(address)) {
				indexOfMember = i;
			}
		}
		if (indexOfMember == -1)
			return null;
		indexOfMember++;
		int foundDistance = 0;
		for (int i = 0; i < size; i++) {
			final MemberImpl member = lsMembers.get((indexOfMember + i) % size);
			if (!(skipSuperClient && member.superClient())) {
				foundDistance++;
			}
			if (foundDistance == distance)
				return member;
		}
		return null;
	}

	protected final MemberImpl getNextMemberBeforeSync(final Address address,
			final boolean skipSuperClient, final int distance) {
		return getNextMemberAfter(ClusterManager.get().getMembersBeforeSync(), address,
				skipSuperClient, distance);
	}

	protected final MemberImpl getPreviousMemberBefore(final Address address,
			final boolean skipSuperClient, final int distance) {
		return getPreviousMemberBefore(lsMembers, address, skipSuperClient, distance);
	}

	protected final MemberImpl getPreviousMemberBefore(final List<MemberImpl> lsMembers,
			final Address address, final boolean skipSuperClient, final int distance) {
		final int size = lsMembers.size();
		if (size <= 1)
			return null;
		int indexOfMember = -1;
		for (int i = 0; i < size; i++) {
			final MemberImpl member = lsMembers.get(i);
			if (member.getAddress().equals(address)) {
				indexOfMember = i;
			}
		}
		if (indexOfMember == -1)
			return null;
		indexOfMember += (size - 1);
		int foundDistance = 0;
		for (int i = 0; i < size; i++) {
			final MemberImpl member = lsMembers.get((indexOfMember - i) % size);
			if (!(skipSuperClient && member.superClient())) {
				foundDistance++;
			}
			if (foundDistance == distance)
				return member;
		}
		return null;
	}

	protected final boolean isMaster() {
		return Node.get().master();
	}

	protected final boolean isSuperClient() {
		return Node.get().isSuperClient();
	}

	protected void log(final Object obj) {
		if (DEBUG)
			logger.log(Level.FINEST, obj.toString());
	}

	protected Packet obtainPacket() {
		final PacketQueue sq = PacketQueue.get();
		return sq.obtainPacket();
	}

	protected final boolean send(final String name, final int operation, final DataSerializable ds,
			final Address address) {
		try {
			final PacketQueue.Packet packet = PacketQueue.get().obtainPacket();
			packet.set(name, operation, null, ds);
			final boolean sent = send(packet, address);
			if (!sent)
				packet.returnToContainer();
			return sent;
		} catch (final Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	protected void sendRedoResponse(final PacketQueue.Packet packet) {
		packet.responseType = RESPONSE_REDO;
		sendResponse(packet);
	}

	protected boolean sendResponse(final PacketQueue.Packet packet) {
		packet.local = false;
		packet.operation = OP_RESPONSE;
		if (packet.responseType == RESPONSE_NONE) {
			packet.responseType = RESPONSE_SUCCESS;
		}
		final boolean sent = send(packet, packet.conn);
		if (!sent) {
			packet.returnToContainer();
		}
		return sent;
	}

	protected boolean sendResponse(final PacketQueue.Packet packet, final Address address) {
		packet.conn = ConnectionManager.get().getConnection(address);
		return sendResponse(packet);
	}

	protected boolean sendResponseFailure(final PacketQueue.Packet packet) {
		packet.local = false;
		packet.operation = OP_RESPONSE;
		packet.responseType = RESPONSE_FAILURE;
		final boolean sent = send(packet, packet.conn);
		if (!sent) { 
			packet.returnToContainer();
		}
		return sent;
	}

	protected boolean sendResponseFailure(final PacketQueue.Packet packet, final Address address) {
		packet.conn = ConnectionManager.get().getConnection(address);
		return sendResponseFailure(packet);
	}

	protected void throwCME(final Object key) {
		throw new ConcurrentModificationException("Another thread holds a lock for the key : "
				+ key);
	}

	void enqueueEvent(final int eventType, final String name, final Data eventKey,
			final Data eventValue, final Address from, final long recordId) {
		final EventTask eventTask = new EventTask(eventType, name, eventKey, eventValue, recordId);
		if (name.startsWith("q:t:")) {
			OrderedEventQueue orderedEventQueue = mapOrderedEventQueues.get(name);
			if (orderedEventQueue == null) {
				orderedEventQueue = new OrderedEventQueue(recordId);
				mapOrderedEventQueues.put(name, orderedEventQueue);
			}
			logger.log(Level.FINE, eventTask.recordId + "offering eventTask " + recordId);
			orderedEventQueue.offer(eventTask);
			executeLocally(orderedEventQueue);
		} else {
			int eventQueueIndex = -1;
			if (eventKey != null) {
				eventQueueIndex = Math.abs(eventKey.hashCode()) % EVENT_QUEUE_COUNT;
			} else {
				eventQueueIndex = Math.abs(from.hashCode()) % EVENT_QUEUE_COUNT;
			}
			final EventQueue eventQueue = eventQueues[eventQueueIndex];
			final int size = eventQueue.offerRunnable (eventTask);
			if (size == 1) executeLocally(eventQueue);
		}
	}

    static class EventQueue extends ConcurrentLinkedQueue<Runnable> implements Runnable {
        private AtomicInteger size = new AtomicInteger();

        public int offerRunnable (Runnable runnable) {
            offer (runnable);
            return size.incrementAndGet();
    	}
		public void run() {
            while (true) {
                final Runnable eventTask = poll();
                if (eventTask != null)   {
                    eventTask.run();
                    size.decrementAndGet();
                } else {
                    return;
                }
            }
		}
	}

	static class EventTask extends EntryEvent implements Runnable {
		protected final Data dataKey;

		protected final Data dataValue;

		protected final long recordId;

		public EventTask(final int eventType, final String name, final Data dataKey,
				final Data dataValue, final long recordId) {
			super(name);
			this.eventType = eventType;
			this.dataValue = dataValue;
			this.dataKey = dataKey;
			this.recordId = recordId;
		}

		public void run() {
			try {
				if (!collection) {
					key = ThreadContext.get().toObject(dataKey);
				}
				if (dataValue != null) {
					value = ThreadContext.get().toObject(dataValue);
				}
				ListenerManager.get().callListeners(this);
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}
	}

	void fireMapEvent(final Map<Address, Boolean> mapListeners, final String name,
			final int eventType, final Object record, final Data oldValue) {
		fireMapEvent(mapListeners, name, eventType, record, oldValue, -1);
	}

	void fireMapEvent(final Map<Address, Boolean> mapListeners, final String name,
			final int eventType, final Object record, final Data oldValue, final long recordId) {
		try {
			// logger.log(Level.FINE,eventType + " FireMapEvent " + record);
			Map<Address, Boolean> mapTargetListeners = null;
			Data dataRecordKey = null;
			Data dataRecordValue = null;
			if (record instanceof Record) {
				final Record rec = (Record) record;
				dataRecordKey = rec.getKey();
				dataRecordValue = rec.getValue();
				if (rec.hasListener()) {
					mapTargetListeners = new HashMap<Address, Boolean>(rec.getMapListeners());
				}
			} else {
				dataRecordValue = (Data) record;
			}
			if (mapListeners != null && mapListeners.size() > 0) {
				if (mapTargetListeners == null) {
					mapTargetListeners = new HashMap<Address, Boolean>(mapListeners);
				} else {
					final Set<Map.Entry<Address, Boolean>> entries = mapListeners.entrySet();
					for (final Map.Entry<Address, Boolean> entry : entries) {
						if (mapTargetListeners.containsKey(entry.getKey())) {
							if (entry.getValue()) {
								mapTargetListeners.put(entry.getKey(), entry.getValue());
							}
						} else
							mapTargetListeners.put(entry.getKey(), entry.getValue());
					}
				}
			}
			if (mapTargetListeners == null || mapTargetListeners.size() == 0) { 
				return;
			}
			final Data key = (dataRecordKey != null) ? ThreadContext.get().hardCopy(dataRecordKey)
					: null;
			Data value = null;
			if (dataRecordValue != null) {
				value = ThreadContext.get().hardCopy(dataRecordValue);
			}
			sendEvents(eventType, name, key, value, mapTargetListeners, recordId);
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

	MemberImpl getMember(final Address address) {
		return ClusterManager.get().getMember(address);
	}

	void handleListenerRegisterations(final boolean add, final String name, final Data key,
			final Address address, final boolean includeValue) {
		if (name.startsWith("q:")) {
			BlockingQueueManager.get().handleListenerRegisterations(add, name, key, address,
					includeValue);
		} else if (name.startsWith("t:")) {
			TopicManager.get().handleListenerRegisterations(add, name, key, address, includeValue);
		} else {
			ConcurrentMapManager.get().handleListenerRegisterations(add, name, key, address,
					includeValue);
		}
	}

	final void handleResponse(final PacketQueue.Packet packetResponse) {
		final Call call = mapCalls.get(packetResponse.callId);
		if (call != null) {
			call.handleResponse(packetResponse);
		} else {
			if (DEBUG) {
				log(packetResponse.operation + " No call for callId " + packetResponse.callId);
			}
			packetResponse.returnToContainer();
		}
	}

	final boolean send(final PacketQueue.Packet packet, final Address address) {
		final Connection conn = ConnectionManager.get().getConnection(address);
		if (conn != null) {
			return writePacket(conn, packet);
		} else {
			return false;
		}
	}

	final boolean send(final PacketQueue.Packet packet, final Connection conn) {
		if (conn != null) {
			return writePacket(conn, packet);
		} else {
			return false;
		} 
	}

	final private boolean writePacket(final Connection conn, final PacketQueue.Packet packet) {
		if (!conn.live()) { 
			return false;
		}
		final MemberImpl memberImpl = getMember(conn.getEndPoint());
		if (memberImpl != null) {
			memberImpl.didWrite();
		}
		packet.currentCallCount = mapCalls.size();
		packet.write();
		conn.getWriteHandler().enqueuePacket(packet);
		return true;
	}

	interface PacketProcessor {
		void process(PacketQueue.Packet packet);
	}
}
