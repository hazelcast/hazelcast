package com.hazelcast.client;

import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.CMap.CMapEntry;

public 	class ClientMapEntry<K, V> implements MapEntry<K, V>{
	private CMapEntry mapEntry;
	private K key;
	private MapClientProxy<K, V> proxy;
	public ClientMapEntry(CMapEntry mapEntry, K key, MapClientProxy<K, V> proxy) {
		this.mapEntry = mapEntry;
		this.key = key;
		this.proxy = proxy;
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
		return proxy.get(key);
	}

	public V setValue(V value) {
		return proxy.put(key,value);
	}
}