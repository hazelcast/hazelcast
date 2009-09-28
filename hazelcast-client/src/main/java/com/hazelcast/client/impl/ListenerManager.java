package com.hazelcast.client.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.hazelcast.client.Packet;
import com.hazelcast.client.Serializer;
import com.hazelcast.client.core.EntryEvent;
import com.hazelcast.client.core.EntryListener;
import com.hazelcast.client.core.EntryEvent.EntryEventType;

public class ListenerManager implements Runnable{
	final Map<String, Map<Object, List<EntryListener>>> mapOfListeners = new HashMap<String, Map<Object,List<EntryListener>>>();
	final BlockingQueue<Packet> queue = new LinkedBlockingQueue<Packet>();
	
	public void registerEntryListener(String name, Object key, EntryListener entryListener){
		if(!mapOfListeners.containsKey(name)){
			mapOfListeners.put(name, new HashMap<Object,List<EntryListener>>());
		}
		if(!mapOfListeners.get(name).containsKey(key)){
			mapOfListeners.get(name).put(key,new ArrayList<EntryListener>());
		}
		mapOfListeners.get(name).get(key).add(entryListener);
	}
	
	private void fireEvent(EntryEvent event){
		String name = event.getName();
		Object key = event.getKey();
		notifyListeners(event, mapOfListeners.get(name).get(null));
		notifyListeners(event, mapOfListeners.get(name).get(key));
	}

	private void notifyListeners(EntryEvent event,
			Collection<EntryListener> collection) {
		if(collection == null){
			return;
		}
		
		for (Iterator<EntryListener> iterator = collection.iterator(); iterator.hasNext();) {
			EntryListener entryListener = iterator.next();
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

	public void run() {
		while(true){
			Packet packet = null;
			try {
				packet = queue.take();
				EntryEvent event = new EntryEvent(packet.getName(),(int)packet.getLongValue(),Serializer.toObject(packet.getKey()),Serializer.toObject(packet.getValue()));
				fireEvent(event);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
}
