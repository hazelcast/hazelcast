package com.hazelcast.client;

import com.hazelcast.core.Instance;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

public class LightEntrySet<K, V> extends AbstractCollection<java.util.Map.Entry<K, V>> implements Set<java.util.Map.Entry<K, V>>{

	private final Set<K> keySet;
	private final MapClientProxy<K,V> proxy;
    private final Instance.InstanceType instanceType;
	public LightEntrySet(Set<K> set, MapClientProxy<K,V> proxy, Instance.InstanceType instanceType) {
		this.keySet = set;
		this.proxy = proxy;
        this.instanceType = instanceType;
	}

	public Iterator<Entry<K, V>> iterator() {
        return new MapEntryIterator<K,V>(keySet.iterator(), proxy, instanceType);
	}

	public int size() {
		return keySet.size();
	}

}