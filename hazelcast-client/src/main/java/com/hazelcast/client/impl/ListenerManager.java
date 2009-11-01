package com.hazelcast.client.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.hazelcast.client.Call;
import com.hazelcast.client.ClientRunnable;
import com.hazelcast.client.Packet;
import com.hazelcast.client.Serializer;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryEvent.EntryEventType;

public class ListenerManager extends ClientRunnable{
	final public Map<String, Map<Object, List<EntryListener>>> mapOfListeners = new HashMap<String, Map<Object,List<EntryListener>>>();
	final private BlockingQueue<Call> listenerCalls = new LinkedBlockingQueue<Call>();
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
	
	public void removeEntryListener(String name, Object key, EntryListener entryListener){
		Map<Object,List<EntryListener>> m = mapOfListeners.get(name);
		if(m!=null){
			List<EntryListener> list =  m.get(key);
			if(list!=null){
				list.remove(entryListener);
				if(m.get(key).size()==0){
					m.remove(key);
				}
			}
			if(m.size()==0){
				mapOfListeners.remove(name);
			}
		}
	}
	
	private void fireEvent(EntryEvent event){
		String name = event.getName();
		Object key = event.getKey();
		System.out.println(event);
		if(mapOfListeners.get(name.substring(2))!=null){
			notifyListeners(event, mapOfListeners.get(name.substring(2)).get(null));
			notifyListeners(event, mapOfListeners.get(name.substring(2)).get(key));
		}
	}

	private void notifyListeners(EntryEvent event, Collection<EntryListener> collection) {
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

	public void addListenerCall(Call call){
		listenerCalls.add(call);
	}
	public BlockingQueue<Call> getListenerCalls(){
		return listenerCalls;
	}

	protected void customRun() throws InterruptedException {
		Packet packet = queue.poll(100, TimeUnit.MILLISECONDS);
		if(packet==null){
			return;
		}
		EntryEvent event = new EntryEvent(packet.getName(),(int)packet.getLongValue(),Serializer.toObject(packet.getKey()),Serializer.toObject(packet.getValue()));
		fireEvent(event);
		
	}
}
