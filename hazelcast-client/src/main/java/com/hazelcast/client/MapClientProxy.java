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

public class MapClientProxy<K, V>  extends ClientProxy implements IMap<K, V>{
	
	final private HazelcastClient client;

	public MapClientProxy(HazelcastClient client, String name) {
		this.name = "c:" + name;
		this.client = client;
	}

	public void addEntryListener(EntryListener<K, V> listener, boolean includeValue) {

		addEntryListener(listener, null, includeValue);
	}

	public void addEntryListener(EntryListener<K, V> listener, K key,
			boolean includeValue) {
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
		return (Boolean) doOp(ClusterOperation.CONCURRENT_MAP_EVICT, Serializer.toByte(key), null);
	}

	public MapEntry getMapEntry(K key) {
		CMapEntry cMapEntry = (CMapEntry)doOp(ClusterOperation.CONCURRENT_MAP_GET_MAP_ENTRY, Serializer.toByte(key), null);
		MapEntry<K, V> mapEntry = new ClientMapEntry(cMapEntry,key);
		return mapEntry;
	}

	class ClientMapEntry implements MapEntry<K, V>{
		private CMapEntry mapEntry;
		private K key;
		public ClientMapEntry(CMapEntry mapEntry, K key) {
			this.mapEntry = mapEntry;
			this.key = key;
		}
		public long getCost() {
			return mapEntry.getCost();
		}

		public long getCreationTime() {
			return mapEntry.getCreationTime();
		}

		public long getExpirationTime() {
			return mapEntry.getExpirationTime();
		}

		public int getHits() {
			return mapEntry.getHits();
		}

		public long getLastAccessTime() {
			return mapEntry.getLastAccessTime();
		}

		public long getLastUpdateTime() {
			return mapEntry.getLastUpdateTime();
		}

		public long getVersion() {
			return mapEntry.getVersion();
		}

		public boolean isValid() {
			return mapEntry.isValid();
		}

		public K getKey() {
			return key;
		}

		public V getValue() {
			return get(key);
		}

		public V setValue(V value) {
			return put(key,value);
		}
		
	}
	
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	public Set<K> keySet(Predicate predicate) {
		// TODO Auto-generated method stub
		return null;
	}

	public void lock(K key) {
		// TODO Auto-generated method stub
		
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
		// TODO Auto-generated method stub
		
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
		return (V)doOp(ClusterOperation.CONCURRENT_MAP_GET, Serializer.toByte(key), null);
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
	public class ClientSet<E> implements Set{
		
		private String name;

		public ClientSet(String name) {
			this.name = name;
		}

		private Set set;

		public ClientSet(Set set) {
			this.set = set;
		}
		
		public boolean add(Object e) {
			return false;
		}

		public boolean addAll(Collection c) {
			// TODO Auto-generated method stub
			return false;
		}

		public void clear() {
			// TODO Auto-generated method stub
			
		}

		public boolean contains(Object o) {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean containsAll(Collection c) {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean isEmpty() {
			// TODO Auto-generated method stub
			return false;
		}

		public Iterator iterator() {
			
			return new ClientIterator<E>(name, set.iterator());
		}

		public boolean remove(Object o) {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean removeAll(Collection c) {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean retainAll(Collection c) {
			// TODO Auto-generated method stub
			return false;
		}

		public int size() {
			// TODO Auto-generated method stub
			return 0;
		}

		public Object[] toArray() {
			// TODO Auto-generated method stub
			return null;
		}

		public Object[] toArray(Object[] a) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	public class ClientIterator<E> implements Iterator<E>{
		private Iterator<?> it;
		private String name;

		public ClientIterator(String name, Iterator<?> it) {
			this.name = name;
			this.it = it;
		}

		public boolean hasNext() {
			return it.hasNext();
		}

		public E next() {
			byte[] key = (byte[])it.next();
			get(key);
			return null;
		}

		public void remove() {
			// TODO Auto-generated method stub
			
		}
		
	}

	public V put(K key, V value) {
		return (V)doOp(ClusterOperation.CONCURRENT_MAP_PUT, Serializer.toByte(key), Serializer.toByte(value));
	}


	public void putAll(Map<? extends K, ? extends V> arg0) {
		// TODO Auto-generated method stub
		
	}

	public V remove(Object arg0) {
		return (V)doOp(ClusterOperation.CONCURRENT_MAP_REMOVE, Serializer.toByte(arg0), null);
	}

	private Object doOp(ClusterOperation operation, byte[] key, byte[] value) {
		Packet request = createRequestPacket(operation, key, value);
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
	class MapGetCall extends MapCall{
		K key;
		V value;
		@Override
		public void fillCallSpecificValues(Packet request) {
			request.setTxnId(0);
		    request.setOperation(ClusterOperation.CONCURRENT_MAP_GET);
		    request.setKey(Serializer.toByte(key));
		}

		@Override
		public Object processResponse(Packet response) {
		if(response.getValue()!=null){
			return (V)Serializer.toObject(response.getValue());
		}
		return null;
		}
		
	}
	abstract class MapCall{
		public abstract void fillCallSpecificValues(Packet request);
		public abstract Object processResponse(Packet response);
		
		public void process(){
			Packet request = createRequestPacket();
			fillCallSpecificValues(request);
			Packet response = callAndGetResult(request);
		}
	}


	public void addIndex(String attribute, boolean ordered) {
		// TODO Auto-generated method stub
		
	}
}
