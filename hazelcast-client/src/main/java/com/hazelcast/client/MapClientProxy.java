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
import java.util.Iterator;
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


import static com.hazelcast.client.Serializer.toByte;
import static com.hazelcast.client.Serializer.toObject;

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
		Packet request = createRequestPacket(ClusterOperation.ADD_LISTENER, toByte(key), null);
		request.setLongValue(includeValue?1:0);
	    Call c = createCall(request);
	    client.listenerManager.addListenerCall(c);
	    doCall(c);
	    client.listenerManager.registerEntryListener(name, key, listener);
	}

	public Set<java.util.Map.Entry<K, V>> entrySet(Predicate predicate) {
		Set<K> keySet = null;
		if(predicate == null){
			keySet = keySet();
		}
		else{
			keySet = keySet(predicate);
		}
		return new LightEntrySet<K, V>(keySet, this);
	}

	public boolean evict(K key) {
		return (Boolean) doOp(ClusterOperation.CONCURRENT_MAP_EVICT, key, null);
	}

	public MapEntry getMapEntry(K key) {
		CMapEntry cMapEntry = (CMapEntry)doOp(ClusterOperation.CONCURRENT_MAP_GET_MAP_ENTRY, key, null);
		MapEntry<K, V> mapEntry = new ClientMapEntry(cMapEntry,key, this);
		return mapEntry;
	}
	
	public Set<K> keySet(Predicate predicate) {
		Packet request = createRequestPacket(ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS, null, toByte(predicate));
	    Packet response = callAndGetResult(request);
	    if(response.getValue()!=null){
	    	Set<K> set = ((Keys<K>)toObject(response.getValue())).getKeys(); 
	    	return set;
	    }
	    return null;	
	}

	public void lock(K key) {
		doLock(ClusterOperation.CONCURRENT_MAP_LOCK, key, -1, null);
	}

	public void removeEntryListener(EntryListener<K, V> listener) {
		removeEntryListener(listener, null);
	}

	public void removeEntryListener(EntryListener<K, V> listener, K key) {
		doOp(ClusterOperation.REMOVE_LISTENER, key, null);
		client.listenerManager.removeEntryListener(name, key, listener);
	}

	public boolean tryLock(K key) {
		return (Boolean)doLock(ClusterOperation.CONCURRENT_MAP_LOCK, key, 0, null);
	}

	public boolean tryLock(K key, long time, TimeUnit timeunit) {
		return (Boolean)doLock(ClusterOperation.CONCURRENT_MAP_LOCK, key, time, timeunit);
	}

	public void unlock(K key) {
		doOp(ClusterOperation.CONCURRENT_MAP_UNLOCK, key, null);
	}

	public Collection<V> values(Predicate predicate) {
		Set<Entry<K,V>> set = entrySet(predicate);
		return new ValueCollection<K,V>(this, set);
	}

	public V putIfAbsent(K arg0, V arg1) {
		return (V)doOp(ClusterOperation.CONCURRENT_MAP_PUT_IF_ABSENT, arg0, arg1);
	}

	public boolean remove(Object arg0, Object arg1) {
		return (Boolean)doOp(ClusterOperation.CONCURRENT_MAP_REMOVE_IF_SAME, arg0, arg1);
	}

	public V replace(K arg0, V arg1) {
		return (V)doOp(ClusterOperation.CONCURRENT_MAP_REPLACE_IF_NOT_NULL, arg0, arg1);
	}

	public boolean replace(K arg0, V arg1, V arg2) {
		Object[] arr = new Object[2];
		arr[0] = arg1;
		arr[1] = arg2;
		return (Boolean)doOp(ClusterOperation.CONCURRENT_MAP_REPLACE_IF_SAME, arg0, arr);
	}

	public void clear() {
		Set keys = keySet();
        for (Object key : keys) {
            remove(key);
        }
	}

	public boolean containsKey(Object arg0) {
		return (Boolean)doOp(ClusterOperation.CONCURRENT_MAP_CONTAINS, arg0, null);
	}

	public boolean containsValue(Object arg0) {
		return (Boolean)doOp(ClusterOperation.CONCURRENT_MAP_CONTAINS_VALUE, null, arg0);
	}

	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return entrySet(null);
	}

	public V get(Object key) {
		return (V)doOp(ClusterOperation.CONCURRENT_MAP_GET, (K)key, null);
	}

	public boolean isEmpty() {
		return size()==0;
	}

	public Set<K> keySet() {
		return keySet(null);
	}

	public V put(K key, V value) {
		return (V)doOp(ClusterOperation.CONCURRENT_MAP_PUT, key, value);
	}


	public void putAll(Map<? extends K, ? extends V> arg0) {
		for(Iterator<? extends K> it = arg0.keySet().iterator(); it.hasNext(); ){
			K key = (K)it.next();
			put((K)key, arg0.get(key));
		}
		
	}

	public V remove(Object arg0) {
		return (V)doOp(ClusterOperation.CONCURRENT_MAP_REMOVE, arg0, null);
	}

	private Object doLock(ClusterOperation operation, Object key, long timeout, TimeUnit timeUnit) {
		Packet request = prepareRequest(operation, key, timeUnit);
		request.setTimeout(timeout);
	    Packet response = callAndGetResult(request);
	    return getValue(response);
	}

	public int size() {
		return (Integer)doOp(ClusterOperation.CONCURRENT_MAP_SIZE, null, null);
	}

	public Collection<V> values() {
		return values(null);
	}

	public void destroy() {
		doOp(ClusterOperation.DESTROY, null, null);
		this.client.destroy(name);
	}

	public Object getId() {
		return doOp(ClusterOperation.GET_ID, null, null);
	}

	public InstanceType getInstanceType() {
		return InstanceType.MAP;
	}

	public void addIndex(String attribute, boolean ordered) {
		doOp(ClusterOperation.ADD_INDEX, attribute, ordered);
		
	}
}
