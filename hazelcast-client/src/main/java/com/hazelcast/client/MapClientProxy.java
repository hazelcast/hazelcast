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

package com.hazelcast.client;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.client.impl.Keys;
import com.hazelcast.query.Predicate;
import com.hazelcast.impl.CMap.CMapEntry;

public class MapClientProxy<K, V>  extends ClientProxy implements IMap<K, V>{
	
	final private HazelcastClient client;

	public MapClientProxy(HazelcastClient client, String name) {
		this.name = name;
		this.client = client;
	}

	public void addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
		addEntryListener(listener, null, includeValue);
	}

	public void addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
		Packet request = createRequestPacket(ClusterOperation.ADD_LISTENER, Serializer.toByte(key), null);
		request.setLongValue(includeValue?1:0);
	    Call c = createCall(request);
	    client.listenerManager.addListenerCall(c);
	    doCall(c);
	    client.listenerManager.registerEntryListener(name, key, listener);
	    
	}

	public Set<java.util.Map.Entry<K, V>> entrySet(Predicate predicate) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean evict(K key) {
		return (Boolean) doOp(ClusterOperation.CONCURRENT_MAP_EVICT, key, null);
	}

	public MapEntry getMapEntry(K key) {
		CMapEntry cMapEntry = (CMapEntry)doOp(ClusterOperation.CONCURRENT_MAP_GET_MAP_ENTRY, key, null);
		MapEntry<K, V> mapEntry = new ClientMapEntry(cMapEntry,key, this);
		return mapEntry;
	}
	
	public String getName() {
		return name;
	}

	public Set<K> keySet(Predicate predicate) {
		// TODO Auto-generated method stub
		return null;
	}

	public void lock(K key) {
		doOp(ClusterOperation.CONCURRENT_MAP_LOCK, key, null);
	}

	public void removeEntryListener(EntryListener<K, V> listener) {
		// TODO Auto-generated method stub
		
	}

	public void removeEntryListener(EntryListener<K, V> listener, K key) {
		// TODO Auto-generated method stub
		
	}

	public boolean tryLock(K key) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean tryLock(K key, long time, TimeUnit timeunit) {
		// TODO Auto-generated method stub
		return false;
	}

	public void unlock(K key) {
		doOp(ClusterOperation.CONCURRENT_MAP_UNLOCK, key, null);
	}

	public Collection<V> values(Predicate predicate) {
		// TODO Auto-generated method stub
		return null;
	}

	public V putIfAbsent(K arg0, V arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean remove(Object arg0, Object arg1) {
		return false;
	}

	public V replace(K arg0, V arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean replace(K arg0, V arg1, V arg2) {
		// TODO Auto-generated method stub
		return false;
	}

	public void clear() {
		// TODO Auto-generated method stub
		
	}

	public boolean containsKey(Object arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean containsValue(Object arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	public Set<java.util.Map.Entry<K, V>> entrySet() {
		// TODO Auto-generated method stub
		return null;
	}

	public V get(Object key) {
		return (V)doOp(ClusterOperation.CONCURRENT_MAP_GET, (K)key, null);
	}

	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	public Set<K> keySet() {
		Packet request = createRequestPacket(ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS, null, null);
	    Packet response = callAndGetResult(request);
	    if(response.getValue()!=null){
	    	System.out.println("Response:" + response.getValue().length);
	    	Set set = ((Keys)Serializer.toObject(response.getValue())).getKeys(); 
	    	return set;
	    }
	    return null;
	    
	}

	public V put(K key, V value) {
		return (V)doOp(ClusterOperation.CONCURRENT_MAP_PUT, key, value);
	}


	public void putAll(Map<? extends K, ? extends V> arg0) {
		// TODO Auto-generated method stub
		
	}

	public V remove(Object arg0) {
		return (V)doOp(ClusterOperation.CONCURRENT_MAP_REMOVE, (K)arg0, null);
	}

	private Object doOp(ClusterOperation operation, K key, V value) {
		byte[] k = null;
		byte[] v = null;
		if(key!=null){
			k= Serializer.toByte(key);
		}
		if(value!=null){
			v= Serializer.toByte(value);
		}
		Packet request = createRequestPacket(operation, k, v);
	    Packet response = callAndGetResult(request);
	    return getValue(response);
	}

	private V getValue(Packet response) {
		if(response.getValue()!=null){
	    	return (V)Serializer.toObject(response.getValue());
	    }
	    return null;
	}

	public int size() {
		return (Integer)doOp(ClusterOperation.CONCURRENT_MAP_SIZE, null, null);
	}

	public Collection<V> values() {
		// TODO Auto-generated method stub
		return null;
	}

	public void destroy() {
		// TODO Auto-generated method stub
		
	}

	public Object getId() {
		// TODO Auto-generated method stub
		return null;
	}

	public InstanceType getInstanceType() {
		// TODO Auto-generated method stub
		return null;
	}

	public void addIndex(String attribute, boolean ordered) {
		// TODO Auto-generated method stub
		
	}
}
