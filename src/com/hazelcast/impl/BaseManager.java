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

import static com.hazelcast.impl.Constants.ClusterOperations.OP_RESPONSE;
import static com.hazelcast.impl.Constants.EventOperations.OP_EVENT;
import static com.hazelcast.impl.Constants.MapTypes.MAP_TYPE_LIST;
import static com.hazelcast.impl.Constants.MapTypes.MAP_TYPE_MAP;
import static com.hazelcast.impl.Constants.MapTypes.MAP_TYPE_SET;
import static com.hazelcast.impl.Constants.Objects.OBJECT_NULL;
import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import static com.hazelcast.impl.Constants.ResponseTypes.RESPONSE_FAILURE;
import static com.hazelcast.impl.Constants.ResponseTypes.RESPONSE_NONE;
import static com.hazelcast.impl.Constants.ResponseTypes.RESPONSE_REDO;
import static com.hazelcast.impl.Constants.ResponseTypes.RESPONSE_SUCCESS;
import static com.hazelcast.impl.Constants.Timeouts.TIMEOUT_ADDITION;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import sun.reflect.ReflectionFactory.GetReflectionFactoryAction;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IMap;
import com.hazelcast.impl.ConcurrentMapManager.Record;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.InvocationQueue;
import com.hazelcast.nio.InvocationQueue.Data;
import com.hazelcast.nio.InvocationQueue.Invocation;

abstract class BaseManager implements Constants {

	protected static ClusterService clusterService;

	protected static Address thisAddress;

	protected static List<MemberImpl> lsMembers = null;

	protected static final boolean DEBUG = Build.get().DEBUG;

	private static long eventId = 1;

	protected static Map<Long, Call> mapCalls = new HashMap<Long, Call>();

	protected static EventQueue[] eventQueues = new EventQueue[100];

	protected static Map<Long, StreamResponseHandler> mapStreams = new ConcurrentHashMap<Long, StreamResponseHandler>();

	protected BaseManager() {
		clusterService = ClusterService.get();
		BaseManager.lsMembers = clusterService.lsMembers;
		thisAddress = Node.get().getThisAddress();
		thisAddress.setThisAddress(true);
		for (int i = 0; i < 100; i++) {
			eventQueues[i] = new EventQueue();
		}
	}

	protected long incrementAndGetEventId() {
		eventId++;
		return eventId;
	}

	protected final void executeLocally(Runnable runnable) {
		ExecutorManager.get().executeLocaly(runnable);
	}

	protected final boolean send(String name, int operation, DataSerializable ds, Address address) {
		try {
			Invocation inv = InvocationQueue.get().obtainInvocation();
			inv.set(name, operation, null, ds);
			boolean sent = send(inv, address);
			if (!sent)
				inv.returnToContainer();
			return sent;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	protected boolean isMaster() {
		return Node.get().master();
	}

	protected Address getMasterAddress() {
		return Node.get().getMasterAddress();
	}

	protected MemberImpl getNextMemberAfter(Address address) {
		return clusterService.getNextMemberAfter(address);
	}

	public MemberImpl getNextMember() {
		return clusterService.getNextMember();
	}

	public MemberImpl getPreviousMember() {
		return clusterService.getPreviousMember();
	}

	public MemberImpl getNextMemberBeforeSync() {
		return clusterService.getNextMemberAfter(ClusterManager.get().getMembersBeforeSync(),
				thisAddress);
	}

	public MemberImpl getNextMemberBeforeSync(Address address) {
		return clusterService.getNextMemberAfter(ClusterManager.get().getMembersBeforeSync(),
				address);
	}

	public boolean newMember(MemberImpl member) {
		return !ClusterManager.get().getMembersBeforeSync().contains(member);
	}

	protected void log(Object obj) {
		if (DEBUG)
			System.out.println(obj);
	}

	protected void redoInvocationLater(Invocation inv) {
		ClusterManager.get().redoInvocationLater(inv);
	}

	protected boolean sendSerializable(String name, int operation, DataSerializable ds,
			Address address) {
		return sendData(name, operation, ThreadContext.get().toData(ds), address);
	}

	protected boolean sendData(String name, int operation, Data data, Address address) {
		Invocation inv = obtainServiceInvocation();
		inv.name = name;
		inv.operation = operation;
		inv.setData(data);
		return send(inv, address);
	}

	protected boolean send(Invocation inv, Address address) {
		return clusterService.send(inv, address);
	}

	protected boolean send(Invocation inv, Connection conn) {
		return clusterService.send(inv, conn);
	}

	protected void sendRedoResponse(Invocation inv) {
		inv.responseType = RESPONSE_REDO;
		sendResponse(inv);
	}

	protected boolean sendResponse(Invocation inv, Address address) {
		Connection conn = ConnectionManager.get().getConnection(address);
		inv.conn = conn;
		return sendResponse(inv);
	}

	protected boolean sendResponse(Invocation inv) {
		inv.local = false;
		inv.operation = OP_RESPONSE;
		if (inv.responseType == RESPONSE_NONE) {
			inv.responseType = RESPONSE_SUCCESS;
		}
		boolean sent = send(inv, inv.conn);
		if (!sent) {
			inv.returnToContainer();
		}
		return sent;
	}

	protected void sendResponseFailure(Invocation inv) {
		inv.local = false;
		inv.operation = OP_RESPONSE;
		inv.responseType = RESPONSE_FAILURE;
		boolean sent = send(inv, inv.conn);
		if (!sent) {
			inv.returnToContainer();
		}
	}

	public Invocation obtainServiceInvocation(String name, Object key, Object value, int operation,
			long timeout) {
		try {
			Invocation inv = obtainServiceInvocation();
			inv.set(name, operation, key, value);
			inv.timeout = timeout;
			return inv;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	protected Invocation obtainServiceInvocation() {
		InvocationQueue sq = InvocationQueue.get();
		return sq.obtainInvocation();
	}

	public abstract class ScheduledAction {
		protected long timeToExpire;

		protected long timeout;

		protected boolean valid = true;

		protected Request request = null;

		public ScheduledAction(Request request) {
			this.request = request;
			setTimeout(request.timeout);
		}

		public void setTimeout(long timeout) {
			this.timeout = timeout;
			if (timeout > -1) {
				timeToExpire = System.currentTimeMillis() + timeout;
			} else
				timeout = -1;
		}

		public boolean isValid() {
			return valid;
		}

		public void setValid(boolean valid) {
			this.valid = valid;
		}

		public long getExpireTime() {
			return timeToExpire;
		}

		public abstract boolean consume();

		public boolean expired() {
			if (!valid)
				return true;
			if (timeout == -1)
				return false;
			else
				return System.currentTimeMillis() >= getExpireTime();
		}

		public boolean neverExpires() {
			return (timeout == -1);
		}

		@Override
		public String toString() {
			return "ScheduledAction {neverExpires=" + neverExpires() + ", timeout= " + timeout
					+ "}";
		}
	}

	public MemberImpl getKeyOwner(Data key) {
		Address ownerAddress = ConcurrentMapManager.get().getKeyOwner(null, key);
		return getMember(ownerAddress);
	}

	MemberImpl getMember(Address address) {
		return ClusterService.get().getMember(address);
	}

	void fireMapEvent(Map<Address, Boolean> mapListeners, String name, int eventType,
			Object record, Data oldValue) {
		try {
			// System.out.println(eventType + " FireMapEvent " + record);
			Map<Address, Boolean> mapTargetListeners = null;
			Data dataRecordKey = null;
			Data dataRecordValue = null;
			if (record instanceof Record) {
				Record rec = (Record) record;
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
					Set<Map.Entry<Address, Boolean>> entries = mapListeners.entrySet();
					for (Map.Entry<Address, Boolean> entry : entries) {
						if (mapTargetListeners.containsKey(entry.getKey())) {
							if (entry.getValue().booleanValue()) {
								mapTargetListeners.put(entry.getKey(), entry.getValue());
							}
						} else
							mapTargetListeners.put(entry.getKey(), entry.getValue());
					}
				}
			}
			if (mapTargetListeners == null || mapTargetListeners.size() == 0)
				return;
			Data key = (dataRecordKey != null) ? ThreadContext.get().hardCopy(dataRecordKey) : null;
			Data value = null;
			if (dataRecordValue != null)
				value = ThreadContext.get().hardCopy(dataRecordValue);
			EventFireTask eventFireTask = new EventFireTask(eventType, name, key, value,
					mapTargetListeners);
			ExecutorManager.get().executeEventFireTask(eventFireTask);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static class EventQueue extends ConcurrentLinkedQueue<Runnable> implements Runnable {
		public void run() {
			Runnable eventTask = poll();
			if (eventTask != null)
				eventTask.run();
		}
	}

	static class EventTask extends EntryEvent implements Runnable {
		protected final Data dataKey;
		protected final Data dataValue;
		protected final String name;

		public EventTask(int eventType, String name, Data dataKey, Data dataValue) {
			super(name);
			this.eventType = eventType;
			this.name = name;
			this.dataValue = dataValue;
			this.dataKey = dataKey;
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
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	void handleListenerRegisterations(boolean add, String name, Data key, Address address,
			boolean includeValue) {
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

	private void handleMapEvent(Invocation inv) {
		int eventType = (int) inv.longValue;
		Data key = inv.doTake(inv.key);
		Data value = inv.doTake(inv.data);
		String name = inv.name;
		Address from = inv.conn.getEndPoint();
		inv.returnToContainer();
		enqueueEvent(eventType, name, key, value, from);
	}

	void enqueueEvent(int eventType, String name, Data eventKey, Data eventValue, Address from) {
		EventTask eventTask = new EventTask(eventType, name, eventKey, eventValue);
		int eventQueueIndex = -1;
		if (eventKey != null) {
			eventQueueIndex = Math.abs(eventKey.hashCode()) % 100;
		} else {
			eventQueueIndex = Math.abs(from.hashCode()) % 100;
		}
		EventQueue eventQueue = eventQueues[eventQueueIndex];
		boolean offered = eventQueue.offer(eventTask);
		if (!offered)
			throw new RuntimeException("event cannot be offered!");
		executeLocally(eventQueue);
	}

	class EventFireTask implements Runnable {
		final Map<Address, Boolean> mapListeners;
		final Data key;
		final Data value;
		final String name;
		final int eventType;

		public EventFireTask(int eventType, String name, Data key, Data value,
				Map<Address, Boolean> mapListeners) {
			super();
			this.eventType = eventType;
			this.name = name;
			this.key = key;
			this.value = value;
			this.mapListeners = mapListeners;
		}

		public void run() {
			if (mapListeners != null) {
				InvocationQueue sq = InvocationQueue.get();
				Set<Map.Entry<Address, Boolean>> entries = mapListeners.entrySet();

				for (Map.Entry<Address, Boolean> entry : entries) {
					Address address = entry.getKey();
					boolean includeValue = entry.getValue();
					// System.out.println(includeValue + " firing event for " +
					// address);
					if (address.isThisAddress()) {
						try {
							Data eventKey = (key != null) ? ThreadContext.get().hardCopy(key)
									: null;
							Data eventValue = null;
							if (includeValue)
								eventValue = ThreadContext.get().hardCopy(value);
							enqueueEvent(eventType, name, eventKey, eventValue, address);
						} catch (Exception e) {
							e.printStackTrace();
						}
					} else {
						Invocation inv = sq.obtainInvocation();
						inv.reset();
						try {
							Data eventKey = key;
							Data eventValue = null;
							if (includeValue)
								eventValue = value;
							inv.set(name, OP_EVENT, eventKey, eventValue);
							inv.longValue = eventType;
						} catch (Exception e) {
							e.printStackTrace();
						}
						boolean sent = clusterService.send(inv, address);
						if (!sent)
							inv.returnToContainer();
					}
				}
			}
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

		public SimpleDataEntry(String name, int blockId, Data key, Data value, int copyCount) {
			super();
			this.blockId = blockId;
			this.keyData = key;
			this.name = name;
			this.valueData = value;
			this.copyCount = copyCount;
		}

		public String getName() {
			return name;
		}

		public int getBlockId() {
			return blockId;
		}

		public Data getKeyData() {
			return keyData;
		}

		public Data getValueData() {
			return valueData;
		}

		public Object getKey() {
			if (key == null) {
				key = ThreadContext.get().toObject(keyData);
			}
			return key;
		}

		public Object getValue() {
			if (value == null) {
				if (valueData != null) {
					value = ThreadContext.get().toObject(valueData);
				}
			}
			return value;
		}

		public Object setValue(Object value) {
			IMap map = (IMap) FactoryImpl.getProxy(name);
			map.put(keyData, value);
			Object oldValue = value;
			this.value = value;
			return oldValue;
		}

		@Override
		public String toString() {
			return "MapEntry key=" + getKey() + ", value=" + getValue();
		}
	}

	public void enqueueAndReturn(Object obj) {
		clusterService.enqueueAndReturn(obj);
	}

	long idGen = 0;

	public long addCall(Call call) {
		long id = idGen++;
		call.setId(id);
		mapCalls.put(id, call);
		return id;
	}

	public Call removeCall(Long id) {
		return mapCalls.remove(id);
	}

	void handleResponse(Invocation invResponse) {
		Call call = mapCalls.get(invResponse.eventId);
		if (call != null)
			call.handleResponse(invResponse);
		else {
			if (DEBUG) {
				log(invResponse.operation + " No call for eventId " + invResponse.eventId);
			}
			invResponse.returnToContainer();
		}
	}

	protected void throwCME(Object key) {
		throw new ConcurrentModificationException("Another thread holds a lock for the key : "
				+ key);
	}

	public void returnScheduledAsBoolean(Request request) {
		if (request.local) {
			TargetAwareOp mop = (TargetAwareOp) request.attachment;
			mop.setResult(request.response);
		} else {
			Invocation inv = request.toInvocation();
			if (request.response == Boolean.TRUE) {
				boolean sent = sendResponse(inv, request.caller);
				if (DEBUG) {
					log(request.local + " returning scheduled response " + sent);
				}
			} else {
				sendResponseFailure(inv);
			}
		}
	}

	public void returnScheduledAsSuccess(Request request) {
		if (request.local) {
			TargetAwareOp mop = (TargetAwareOp) request.attachment;
			mop.setResult(request.response);
		} else {
			Invocation inv = request.toInvocation();
			Object result = request.response;
			if (result != null) {
				if (result instanceof Data) {
					Data oldValue = (Data) result;
					if (oldValue != null && oldValue.size() > 0) {
						inv.doSet(oldValue, inv.data);
					}
				}
			}
			sendResponse(inv, request.caller);
		}
	}

	abstract class QueueBasedCall extends AbstractCall {
		final protected BlockingQueue responses;

		public QueueBasedCall() {
			this(true);
		}

		public QueueBasedCall(boolean limited) {
			if (limited) {
				responses = new ArrayBlockingQueue(1);
			} else {
				responses = new LinkedBlockingQueue();
			}
		}

		@Override
		public void redo() {
			removeCall(getId());
			responses.clear();
			responses.add(OBJECT_REDO);
		}

		void setResult(Object obj) {
			if (obj == null) {
				responses.add(OBJECT_NULL);
			} else {
				responses.add(obj);
			}
		}

		public void handleBooleanNoneRedoResponse(Invocation inv) {
			removeCall(getId());
			if (inv.responseType == ResponseTypes.RESPONSE_SUCCESS) {
				responses.add(Boolean.TRUE);
			} else {
				responses.add(Boolean.FALSE);
			}
		}

		void handleObjectNoneRedoResponse(Invocation inv) {
			removeCall(getId());
			if (inv.responseType == ResponseTypes.RESPONSE_SUCCESS) {
				Data oldValue = inv.doTake(inv.data);
				if (oldValue == null || oldValue.size() == 0) {
					responses.add(OBJECT_NULL);
				} else {
					responses.add(oldValue);
				}
			} else {
				throw new RuntimeException("responseType " + inv.responseType);
			}
		}
	}

	abstract class ResponseQueueCall extends RequestBasedCall {
		final protected BlockingQueue responses;

		public ResponseQueueCall() {
			this(true);
		}

		public ResponseQueueCall(boolean limited) {
			if (limited) {
				responses = new ArrayBlockingQueue(1);
			} else {
				responses = new LinkedBlockingQueue();
			}
		}

		@Override
		public void redo() {
			removeCall(getId());
			responses.clear();
			responses.add(OBJECT_REDO);
		}

		void setResult(Object obj) {
			if (obj == null) {
				responses.add(OBJECT_NULL);
			} else {
				responses.add(obj);
			}
		}

		public void handleBooleanNoneRedoResponse(Invocation inv) {
			removeCall(getId());
			if (inv.responseType == ResponseTypes.RESPONSE_SUCCESS) {
				responses.add(Boolean.TRUE);
			} else {
				responses.add(Boolean.FALSE);
			}
		}

		void handleObjectNoneRedoResponse(Invocation inv) {
			removeCall(getId());
			if (inv.responseType == ResponseTypes.RESPONSE_SUCCESS) {
				Data oldValue = inv.doTake(inv.data);
				if (oldValue == null || oldValue.size() == 0) {
					responses.add(OBJECT_NULL);
				} else {
					responses.add(oldValue);
				}
			} else {
				throw new RuntimeException("responseType " + inv.responseType);
			}
		}
	}

	abstract class BooleanOp extends TargetAwareOp {
		@Override
		void handleNoneRedoResponse(Invocation inv) {
			handleBooleanNoneRedoResponse(inv);
		}
	}

	abstract class TargetAwareOp extends ResponseQueueCall {

		Address target = null;

		public TargetAwareOp() {
		}

		void handleNoneRedoResponse(Invocation inv) {
			handleObjectNoneRedoResponse(inv);
		}

		public void handleResponse(Invocation inv) {
			if (inv.responseType == RESPONSE_REDO) {
				redo();
			} else {
				handleNoneRedoResponse(inv);
			}
			inv.returnToContainer();
		}

		@Override
		public void onDisconnect(Address dead) {
			if (dead.equals(target)) {
				redo();
			}
		}

		@Override
		public void doOp() {
			responses.clear();
			enqueueAndReturn(TargetAwareOp.this); 
		}

		public Object getResult() {
			long timeout = request.timeout;
			Object result = null;
			try {
				if (timeout == -1 ) {
					result = responses.take();					
				} else {
					result = responses.poll(timeout + TIMEOUT_ADDITION, TimeUnit.MILLISECONDS);
				}
				if (result == OBJECT_REDO) {
					Thread.sleep(2000);
					// if (DEBUG) {
					// log(getId() + " Redoing.. " + this);
					// }
					request.redoCount ++;
					doOp();
					return getResult();
				}
			} catch (Exception e) {
				e.printStackTrace(System.out);
			}
			return result;
		}

		abstract void setTarget();

		public void process() {
			setTarget();
			if (target.equals(thisAddress)) {
				doLocalOp();
			} else {
				addCall(TargetAwareOp.this);
				Invocation inv = request.toInvocation();
				inv.eventId = getId();
				boolean sent = send(inv, target);
				if (!sent) {
					ConnectionManager.get().getOrConnect(target);
					if (DEBUG) {
						log("invocation cannot be sent to " + target);
					}
					inv.returnToContainer();
					redo();
				}
			}
		}

		abstract void doLocalOp();
	}

	abstract class AllOp extends RequestBasedCall {
		int numberOfExpectedResponses = 0;
		int numberOfResponses = 0;
		protected boolean done = false;
		protected Set<Address> setAddresses = new HashSet<Address>();

		public AllOp() {
		}

		@Override
		void doOp() {
			reset();
			try {
				synchronized (this) {
					enqueueAndReturn(this);
					wait();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public Object getResult() {
			return null;
		}

		public void process() {
			doLocalOp();
			if (setAddresses.size() == 0) {
				for (MemberImpl member : lsMembers) {
					setAddresses.add(member.getAddress());
				}
			}
			numberOfResponses = 1;
			numberOfExpectedResponses = setAddresses.size();
			if (setAddresses.size() > 1) {
				addCall(AllOp.this);
				for (Address address : setAddresses) {
					if (!address.equals(thisAddress)) {
						Invocation inv = request.toInvocation();
						inv.eventId = getId();
						boolean sent = send(inv, address);
						if (!sent) {
							inv.returnToContainer();
							log(address + " not reachable: operation redoing:  " + AllOp.this);
							redo();
						}

					}
				}
			} else {
				complete(false);
			}
		}

		@Override
		public void onDisconnect(Address dead) {
			log(dead + " onDisconnect " + AllOp.this);
			reset();
			redo();
		}

		public void reset() {
			done = false;
			numberOfResponses = 0;
			numberOfExpectedResponses = 0;
			setAddresses.clear();
		}

		abstract void doLocalOp();

		void consumeResponse(Invocation inv) {
			numberOfResponses++;
			if (numberOfResponses >= numberOfExpectedResponses) {
				complete(true);
			}
			inv.returnToContainer();
		}

		public void handleResponse(Invocation inv) {
			consumeResponse(inv);
		}

		void complete(boolean call) {
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
	}

	abstract class RequestBasedCall extends AbstractCall {
		final protected Request request = new Request();

		public void setLocal(int operation, String name, Object key, Object value, long timeout,
				long txnId, long recordId) {
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

		public Object objectCall() {
			doOp();
			return getResultAsObject();
		}

		public Object objectCall(int operation, String name, Object key, Object value,
				long timeout, long txnId, long recordId) {
			setLocal(operation, name, key, value, timeout, txnId, recordId);
			return objectCall();
		}

		public boolean booleanCall(int operation, String name, Object key, Object value,
				long timeout, long txnId, long recordId) {
			doOp(operation, name, key, value, timeout, txnId, recordId);
			return getResultAsBoolean();
		}

		abstract void doOp();

		abstract Object getResult();

		public void doOp(int operation, String name, Object key, Object value, long timeout,
				long txnId, long recordId) {
			setLocal(operation, name, key, value, timeout, txnId, recordId);
			doOp();
		}

		public Object getResultAsObject() {
			try {
				Object result = getResult();
				if (result == OBJECT_NULL || result == null) {
					return null;
				}
				if (result instanceof Data) {
					Data data = (Data) result;
					if (data.size() == 0)
						return null;
					return ThreadContext.get().toObject(data);
				}
				return result;
			} catch (Throwable e) {
				if (DEBUG) {
					ClusterManager.get().publishLog(e.toString());
				}
				e.printStackTrace(System.out);
			} finally {
				request.reset();
			}
			return null;
		}

		public boolean getResultAsBoolean() {
			try {
				Object result = getResult();
				if (result == OBJECT_NULL || result == null) {
					return false;
				}
				if (result == Boolean.TRUE)
					return true;
				else
					return false;
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				this.request.reset();
			}
			return false;
		}

	}

	interface Call extends Processable {

		void handleResponse(Invocation inv);

		void setId(long id);

		long getId();

		void onDisconnect(Address dead);
	}

	abstract class AbstractCall implements Call {
		private long id = -1;

		public AbstractCall() {
		}

		public void setId(long id) {
			this.id = id;
		}

		public long getId() {
			return id;
		}

		public void redo() {
			removeCall(getId());
			id = -1;
			enqueueAndReturn(this);
		}

		public void onDisconnect(Address dead) {
		}
	}

	class Request {
		volatile int redoCount =0;
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

		Object attachment = null;
		Object response = null;
		boolean scheduled = false;

		public Request softCopy() {
			Request copy = new Request();
			copy.set(local, operation, name, key, value, blockId, timeout, txnId, eventId,
					lockThreadId, lockAddress, lockCount, caller, longValue, recordId);
			copy.attachment = attachment;
			copy.response = response;
			copy.scheduled = scheduled;
			return copy;
		}

		public boolean hasEnoughTimeToSchedule() {
			return timeout == -1 || timeout > 100;
		}

		public void setLocal(int operation, String name, Data key, Data value, int blockId,
				long timeout, long recordId) {
			reset();
			set(true, operation, name, key, value, blockId, timeout, -1, -1, -1, thisAddress, 0,
					thisAddress, -1, recordId);
			this.txnId = ThreadContext.get().getTxnId();
			this.lockThreadId = Thread.currentThread().hashCode();
			this.caller = thisAddress;
		}

		public void setInvocation(Invocation inv) {
			reset();
			set(false, inv.operation, inv.name, inv.doTake(inv.key), inv.doTake(inv.data),
					inv.blockId, inv.timeout, inv.txnId, inv.eventId, inv.threadId,
					inv.lockAddress, inv.lockCount, inv.conn.getEndPoint(), inv.longValue,
					inv.recordId);

		}

		public void set(boolean local, int operation, String name, Data key, Data value,
				int blockId, long timeout, long txnId, long eventId, int lockThreadId,
				Address lockAddress, int lockCount, Address caller, long longValue, long recordId) {
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
		}

		public Invocation toInvocation() {
			Invocation inv = obtainServiceInvocation();
			inv.local = false;
			inv.operation = operation;
			inv.name = name;
			if (key != null)
				inv.doHardCopy(key, inv.key);
			if (value != null)
				inv.doHardCopy(value, inv.data);
			inv.blockId = blockId;
			inv.timeout = timeout;
			inv.txnId = txnId;
			inv.eventId = eventId;
			inv.threadId = lockThreadId;
			inv.lockAddress = lockAddress;
			inv.lockCount = lockCount;
			inv.longValue = longValue;
			inv.recordId = recordId;
			return inv;
		}

		public void reset() {
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
		}
	}

	interface Processable {
		void process();
	}

	public static byte getMapType(String name) {
		byte mapType = MAP_TYPE_MAP;
		if (name.length() > 3) {
			String typeStr = name.substring(2, 4);
			if ("s:".equals(typeStr))
				mapType = MAP_TYPE_SET;
			else if ("l:".equals(typeStr))
				mapType = MAP_TYPE_LIST;
		}
		return mapType;
	}

}
