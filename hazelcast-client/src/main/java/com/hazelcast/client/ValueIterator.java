package com.hazelcast.client;

import java.util.Iterator;
import java.util.Map.Entry;

public class ValueIterator<K,V> implements Iterator<V>{
	
	private final MapClientProxy<K, V> proxy;
	private final Iterator<Entry<K, V>> entryIterator;

	public ValueIterator(MapClientProxy<K, V> proxy, Iterator<Entry<K,V>> entryIterator) {
		this.proxy = proxy;
		this.entryIterator = entryIterator;
	}

	public boolean hasNext() {
		return entryIterator.hasNext();
	}

	public V next() {
		V next = null;
		while((next = entryIterator.next().getValue())!=null);
		return next;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
	
};