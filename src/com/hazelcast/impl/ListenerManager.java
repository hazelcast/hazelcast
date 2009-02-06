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

import static com.hazelcast.impl.Constants.EventOperations.OP_EVENT;
import static com.hazelcast.impl.Constants.EventOperations.OP_LISTENER_ADD;
import static com.hazelcast.impl.Constants.EventOperations.OP_LISTENER_REMOVE;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.MessageListener;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.InvocationQueue.Data;
import com.hazelcast.nio.InvocationQueue.Invocation;

class ListenerManager extends BaseManager {
	List<ListenerItem> lsListeners = new CopyOnWriteArrayList<ListenerItem>();
	public static final int LISTENER_TYPE_MAP = 1;
	public static final int LISTENER_TYPE_ITEM = 2;
	public static final int LISTENER_TYPE_MESSAGE = 3;
	private static final ListenerManager instance = new ListenerManager();

	public static ListenerManager get() {
		return instance;
	}

	private ListenerManager() {
	}

	public void handle(Invocation inv) {
		if (inv.operation == OP_EVENT) {
			handleEvent(inv);
		} else if (inv.operation == OP_LISTENER_ADD) {
			handleAddRemoveListener(true, inv);
		} else if (inv.operation == OP_LISTENER_REMOVE) {
			handleAddRemoveListener(false, inv);
		} else
			throw new RuntimeException("Unknown operation " + inv.operation);
	}

	private final void handleEvent(Invocation inv) {
		int eventType = (int) inv.longValue;
		Data key = inv.doTake(inv.key);
		Data value = inv.doTake(inv.data);
		String name = inv.name;
		Address from = inv.conn.getEndPoint();
		long recordId = inv.recordId;
		inv.returnToContainer();
		enqueueEvent(eventType, name, key, value, from, recordId);
	}

	private final void handleAddRemoveListener(boolean add, Invocation inv) {
		Data key = (inv.key != null) ? inv.doTake(inv.key) : null;
		boolean returnValue = (inv.longValue == 1) ? true : false;
		String name = inv.name;
		Address address = inv.conn.getEndPoint();
		inv.returnToContainer();
		handleListenerRegisterations(add, name, key, address, returnValue);
	}

	public void syncForDead(Address deadAddress) {
		syncForAdd();
	}

	public void syncForAdd() {
//		for (ListenerItem listenerItem : lsListeners) {
//			registerListener(listenerItem.name, listenerItem.key, true, listenerItem.includeValue);
//		}
	}
	
	public void syncForAdd(Address newAddress) {
		for (ListenerItem listenerItem : lsListeners) {
			Data dataKey = null;
			if (listenerItem.key != null) {
				try {
					dataKey = ThreadContext.get().toData(listenerItem.key);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			sendAddRemoveListener(newAddress, true, listenerItem.name, dataKey, listenerItem.includeValue);
		}
	}

	/**
	 * user thread calls this
	 * 
	 * @param name
	 * @param key
	 */
	private void registerListener(String name, Object key, boolean add, boolean includeValue) {
		Data dataKey = null;
		if (key != null) {
			try {
				dataKey = ThreadContext.get().toData(key);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		enqueueAndReturn(new ListenerRegistrationProcess(name, dataKey, add, includeValue));
	}

	class ListenerRegistrationProcess implements Processable {
		String name;
		Data key;
		boolean add = true;
		int invocationProcess = OP_LISTENER_ADD;
		boolean includeValue = true;

		public ListenerRegistrationProcess(String name, Data key, boolean add, boolean includeValue) {
			super();
			this.key = key;
			this.name = name;
			this.add = add;
			this.includeValue = includeValue;
			if (!add)
				invocationProcess = OP_LISTENER_REMOVE;
		}

		public void process() {
			try {
				if (key != null) {
					Address owner = ConcurrentMapManager.get().getKeyOwner(name, key);
					if (owner.equals(thisAddress)) {
						handleListenerRegisterations(add, name, key, thisAddress, includeValue);
					} else {
						Invocation inv = obtainServiceInvocation();
						inv.set(name, invocationProcess, key, null);
						inv.longValue = (includeValue) ? 1 : 0;
						boolean sent = send(inv, owner);
						if (!sent) {
							inv.returnToContainer();
						}
					}
				} else {
					for (MemberImpl member : lsMembers) {
						if (member.localMember()) {
							handleListenerRegisterations(add, name, key, thisAddress, includeValue);
						} else {
							sendAddRemoveListener(member.getAddress(), add, name, key, includeValue);
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void sendAddRemoveListener(Address toAddress, boolean add, String name, Data key,
			boolean includeValue) {
		Invocation inv = obtainServiceInvocation();
		try {
			inv.set(name, (add) ? OP_LISTENER_ADD : OP_LISTENER_REMOVE, key, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		inv.longValue = (includeValue) ? 1 : 0;
		boolean sent = send(inv, toAddress);
		if (!sent) {
			inv.returnToContainer();
		}
	}

	public void addListener(String name, Object listener, Object key, boolean includeValue,
			int listenerType) {
		addListener(name, listener, key, includeValue, listenerType, true);
	}

	public synchronized void addListener(String name, Object listener, Object key, boolean includeValue,
			int listenerType, boolean shouldRemotelyRegister) {
		/**
		 * check if already registered send this address to the key owner as a
		 * listener add this listener to the local listeners map
		 */
		if (shouldRemotelyRegister) {
			boolean remotelyRegister = true;
			for (ListenerItem listenerItem : lsListeners) {
				if (remotelyRegister) {
					if (listenerItem.listener == listener) {
						if (listenerItem.name.equals(name)) {
							if (key == null) {
								if (listenerItem.key == null) {
									if (!includeValue || listenerItem.includeValue == includeValue) {
										remotelyRegister = false;
									}
								}
							} else {
								if (listenerItem.key != null) {
									if (listenerItem.key.equals(key)) {
										if (!includeValue
												|| listenerItem.includeValue == includeValue) {
											remotelyRegister = false;
										}
									}
								}
							}
						}
					}
				}
			}
			if (remotelyRegister) {
				registerListener(name, key, true, includeValue);
			}
		}
		ListenerItem listenerItem = new ListenerItem(name, key, listener, includeValue,
				listenerType);
		lsListeners.add(listenerItem);
	}

	public synchronized void removeListener(String name, Object listener, Object key) {
		/**
		 * send this address to the key owner as a listener add this listener to
		 * the local listeners map
		 */

		Iterator<ListenerItem> it = lsListeners.iterator();
		for (; it.hasNext();) {
			ListenerItem listenerItem = it.next();
			if (listener == listenerItem.listener) {
				if (key == null) {
					if (listenerItem.key == null) {
						registerListener(name, null, false, false);
						it.remove();
					}
				} else if (key.equals(listenerItem.key)) {
					registerListener(name, key, false, false);
					it.remove();
				}
			}
		}
	}

	public synchronized void removeMapListener(Object listener) {
		for (ListenerItem listenerItem : lsListeners) {
			if (listenerItem.listener == listener) {
				lsListeners.remove(listenerItem);
			}
		}
	}

	void callListeners(EventTask event) {
		String name = event.name;
		for (ListenerItem listenerItem : lsListeners) {
			if (listenerItem.name.equals(name)) {
				if (listenerItem.key == null) {
					callListener(listenerItem, event);
				} else if (event.getKey().equals(listenerItem.key)) {
					callListener(listenerItem, event);
				}
			}
		}
	}

	private void callListener(ListenerItem listenerItem, EntryEvent event) {
		Object listener = listenerItem.listener;
		if (listenerItem.listenerType == LISTENER_TYPE_MAP) {
			EntryListener l = (EntryListener) listener;
			EntryEvent e = event;
			if (event.getEventType() == EntryEvent.TYPE_ADDED)
				l.entryAdded(e);
			else if (event.getEventType() == EntryEvent.TYPE_REMOVED)
				l.entryRemoved(e);
			else if (event.getEventType() == EntryEvent.TYPE_UPDATED)
				l.entryUpdated(e);
		} else if (listenerItem.listenerType == LISTENER_TYPE_ITEM) {
			ItemListener l = (ItemListener) listener;
			if (event.getEventType() == EntryEvent.TYPE_ADDED)
				l.itemAdded(event.getValue());
			else if (event.getEventType() == EntryEvent.TYPE_REMOVED)
				l.itemRemoved(event.getValue());
		} else if (listenerItem.listenerType == LISTENER_TYPE_MESSAGE) {
			MessageListener l = (MessageListener) listener;
			l.onMessage(event.getValue());
		}
	}

	class ListenerItem {
		public String name;
		public Object key;
		public Object listener;
		public boolean includeValue;
		public int listenerType = ListenerManager.LISTENER_TYPE_MAP;

		public ListenerItem(String name, Object key, Object listener, boolean includeValue,
				int listenerType) {
			super();
			this.key = key;
			this.listener = listener;
			this.name = name;
			this.includeValue = includeValue;
			this.listenerType = listenerType;
		}

	}

}
