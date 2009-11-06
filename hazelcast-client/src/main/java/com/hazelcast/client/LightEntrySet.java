package com.hazelcast.client;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

public class LightEntrySet<K, V> extends AbstractCollection<java.util.Map.Entry<K, V>> implements Set<java.util.Map.Entry<K, V>>{

	private String name;
	private Set<K> keySet;
	private MapClientProxy<K,V> proxy;
	public LightEntrySet(Set<K> set, MapClientProxy<K,V> proxy) {
		this.keySet = set;
		this.proxy = proxy;
	}

	public Iterator<Entry<K, V>> iterator() {
		return new MapEntryIterator<K,V>(name, keySet.iterator(), proxy);
	}

	public int size() {
		return keySet.size();
	}

}