/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.client.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.hazelcast.client.Call;
import com.hazelcast.client.ClientRunnable;
import com.hazelcast.client.Packet;
import com.hazelcast.core.*;
import com.hazelcast.core.EntryEvent.EntryEventType;
import com.sun.swing.internal.plaf.synth.resources.synth;

import static com.hazelcast.client.Serializer.toObject;
import static com.hazelcast.impl.BaseManager.getInstanceType;

public class ListenerManager extends ClientRunnable{
	final public Map<String, Map<Object, List<EntryListener<?,?>>>> entryListeners = new ConcurrentHashMap<String, Map<Object,List<EntryListener<?,?>>>>();
	final public Map<String, List<MessageListener<Object>>> messageListeners = new HashMap<String, List<MessageListener<Object>>>();
    final private BlockingQueue<Call> listenerCalls = new LinkedBlockingQueue<Call>();
	final public Map<String, Map<Object, Call>> callMap = new ConcurrentHashMap<String, Map<Object,Call>>();
	final Map<ItemListener, EntryListener> itemListener2EntryListener = new ConcurrentHashMap<ItemListener, EntryListener>();
	final BlockingQueue<Packet> queue = new LinkedBlockingQueue<Packet>();

	
	public synchronized void registerEntryListener(String name, Object key, EntryListener<?,?> entryListener){
		if(!entryListeners.containsKey(name)){
			entryListeners.put(name, new HashMap<Object,List<EntryListener<?,?>>>());
		}
		if(!entryListeners.get(name).containsKey(key)){
			entryListeners.get(name).put(key,new ArrayList<EntryListener<?,?>>());
		}
		entryListeners.get(name).get(key).add(entryListener);
	}

    public synchronized <E, V> void registerItemListener(String name, final ItemListener<E> itemListener){

		EntryListener<E,V> e = new EntryListener<E,V>(){
			public void entryAdded(EntryEvent<E,V> event) {
				itemListener.itemAdded((E)event.getKey());
			}

			public void entryEvicted(EntryEvent<E,V> event) {
				// TODO Auto-generated method stub
			}

			public void entryRemoved(EntryEvent<E,V> event) {
				itemListener.itemRemoved((E)event.getKey());
			}

			public void entryUpdated(EntryEvent<E,V> event) {
				// TODO Auto-generated method stub
			}
	    };
	    registerEntryListener(name, null, e);
	    itemListener2EntryListener.put(itemListener, e);

	}

    public synchronized void registerMessageListener(String name, MessageListener messageListener) {
        if(!messageListeners.containsKey(name)){
            messageListeners.put(name, new ArrayList<MessageListener<Object>>());
        }
        messageListeners.get(name).add(messageListener);
    }

    public synchronized void removeMessageListener(String name, MessageListener messageListener) {
        if(!messageListeners.containsKey(name)){
            return;
        }
        messageListeners.get(name).remove(messageListener);
        if(messageListeners.get(name).size()==0){
            messageListeners.remove(name);
        }
    }

    public boolean noMessageListenerRegistered(String name){
        if(!messageListeners.containsKey(name)){
            return true;
        }
        return messageListeners.get(name).size()<=0;
    }

    public boolean noEntryListenerRegistered(Object key, String name) {
        return !(entryListeners.get(name)!=null &&
				entryListeners.get(name).get(key)!=null &&
				entryListeners.get(name).get(key).size()>0);
    }

    public synchronized void removeEntryListener(String name, Object key, EntryListener<?,?> entryListener){
		Map<Object,List<EntryListener<?,?>>> m = entryListeners.get(name);
		if(m!=null){
			List<EntryListener<?,?>> list =  m.get(key);
			if(list!=null){
				list.remove(entryListener);
				if(m.get(key).size()==0){
					m.remove(key);
					removeListenerCall(name, key);
				}
			}
			if(m.size()==0){
				entryListeners.remove(name);
			}
		}
	}

    public synchronized void removeItemListener(String name, ItemListener itemListener){
		EntryListener entryListener = itemListener2EntryListener.remove(itemListener);
		removeEntryListener(name, null, entryListener);

	}

    private void removeListenerCall(String name, Object key) {
		callMap.get(name).remove(key);
		if(callMap.get(name).size()==0){
			callMap.remove(name);
		}
	}

    private void fireEntryEvent(EntryEvent event){
		String name = event.getName();
		Object key = event.getKey();
		if(entryListeners.get(name)!=null){
			notifyEntryListeners(event, entryListeners.get(name).get(null));
			notifyEntryListeners(event, entryListeners.get(name).get(key));
		}
	}

    private void notifyEntryListeners(EntryEvent event, Collection<EntryListener<?,?>> collection) {
		if(collection == null){
			return;
		}
		for (Iterator<EntryListener<?,?>> iterator = collection.iterator(); iterator.hasNext();) {
			EntryListener<?,?> entryListener = iterator.next();
			if(event.getEventType().equals(EntryEventType.ADDED)){
				entryListener.entryAdded(event);
			}
			else if(event.getEventType().equals(EntryEventType.REMOVED)){
				entryListener.entryRemoved(event);
			}
			else if(event.getEventType().equals(EntryEventType.UPDATED)){
				entryListener.entryUpdated(event);
			}
			else if(event.getEventType().equals(EntryEventType.EVICTED)){
				entryListener.entryEvicted(event);
			}
		}
	}

    public void enqueue(Packet packet){
		try {
			queue.put(packet);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

    public synchronized void addListenerCall(Call call, String name, Object key){
		listenerCalls.add(call);
		if(!callMap.containsKey(name)){
			callMap.put(name, new HashMap<Object,Call>());
		}
		if(callMap.get(name).containsKey(key)){
			throw new RuntimeException("There should be mostly one call per (map, key)");
		}
		callMap.get(name).put(key,call);
	}

    public BlockingQueue<Call> getListenerCalls(){
		return listenerCalls;
	}

    protected void customRun() throws InterruptedException {
		Packet packet = queue.poll(100, TimeUnit.MILLISECONDS);
		if(packet==null){
			return;
		}
        if(getInstanceType(packet.getName()).equals(Instance.InstanceType.TOPIC)){
            notifyMessageListeners(packet);
        }
        else{
            notifyEventListeners(packet);
        }

	}

    private void notifyEventListeners(Packet packet) {
        EntryEvent event = new EntryEvent(packet.getName(),(int)packet.getLongValue(),toObject(packet.getKey()),toObject(packet.getValue()));
        fireEntryEvent(event);
    }

    private void notifyMessageListeners(Packet packet) {
        List<MessageListener<Object>> list = messageListeners.get(packet.getName());
        if(list!=null){
                for (Iterator<MessageListener<Object>> it = list.iterator();it.hasNext();){
                MessageListener messageListener = it.next();
                messageListener.onMessage(toObject(packet.getKey()));
            }
        }
    }
}
