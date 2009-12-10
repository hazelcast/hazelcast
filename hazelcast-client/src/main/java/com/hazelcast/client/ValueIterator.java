package com.hazelcast.client;

import java.util.Iterator;
import java.util.Map.Entry;

public class ValueIterator<K,V> implements Iterator<V>{
	
	private final Iterator<Entry<K, V>> entryIterator;

	public ValueIterator(Iterator<Entry<K,V>> entryIterator) {
		this.entryIterator = entryIterator;
	}

	public boolean hasNext() {
		return entryIterator.hasNext();
	}

	public V next() {
		V next = entryIterator.next().getValue();
        if(next==null){
            System.out.println("Next is null");
            next = next();
        }
		return next;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
	
};