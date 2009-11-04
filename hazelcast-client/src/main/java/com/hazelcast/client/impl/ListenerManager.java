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
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.EntryEvent.EntryEventType;
import com.hazelcast.core.Instance.InstanceType;
import com.hazelcast.impl.BaseManager;

import static com.hazelcast.client.Serializer.toObject;

public class ListenerManager extends ClientRunnable{
	final public Map<String, Map<Object, List<EntryListener<?,?>>>> entryListeners = new HashMap<String, Map<Object,List<EntryListener<?,?>>>>();
	final public Map<String, List<ItemListener<?>>> itemListeners = new HashMap<String, List<ItemListener<?>>>();
	final private BlockingQueue<Call> listenerCalls = new LinkedBlockingQueue<Call>();
	final BlockingQueue<Packet> queue = new LinkedBlockingQueue<Packet>();
	
	public void registerEntryListener(String name, Object key, EntryListener<?,?> entryListener){
		if(!entryListeners.containsKey(name)){
			entryListeners.put(name, new HashMap<Object,List<EntryListener<?,?>>>());
		}
		if(!entryListeners.get(name).containsKey(key)){
			entryListeners.get(name).put(key,new ArrayList<EntryListener<?,?>>());
		}
		entryListeners.get(name).get(key).add(entryListener);
	}
	
	public void registerItemListener(String name, ItemListener<?> itemListener){
		if(!itemListeners.containsKey(name)){
			itemListeners.put(name, new ArrayList<ItemListener<?>>());
		}
		itemListeners.get(name).add(itemListener);
	}
	
	public void removeEntryListener(String name, Object key, EntryListener<?,?> entryListener){
		Map<Object,List<EntryListener<?,?>>> m = entryListeners.get(name);
		if(m!=null){
			List<EntryListener<?,?>> list =  m.get(key);
			if(list!=null){
				list.remove(entryListener);
				if(m.get(key).size()==0){
					m.remove(key);
				}
			}
			if(m.size()==0){
				entryListeners.remove(name);
			}
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
//		InstanceType instanceType = BaseManager.getInstanceType(packet.getName()); 
//		if(InstanceType.MAP.equals(instanceType)){
			EntryEvent event = new EntryEvent(packet.getName(),(int)packet.getLongValue(),toObject(packet.getKey()),toObject(packet.getValue()));
			fireEntryEvent(event);
//		}
//		else if(InstanceType.LIST.equals(instanceType)){
			
			
//		}
		
	}
}
