package com.hazelcast.client;

import java.util.Iterator;
import java.util.Map.Entry;

public class MapEntryIterator<K, V> implements Iterator<java.util.Map.Entry<K, V>>{
	private final Iterator<K> it;
	private final MapClientProxy<K,V> proxy;
	private volatile Entry<K,V> lastEntry;
	
	
	public MapEntryIterator(Iterator<K> it, MapClientProxy<K,V> proxy) {
		this.it = it;
		this.proxy = proxy;
	}

	public boolean hasNext() {
		return it.hasNext();
	}

	public Entry<K,V> next() {
		K key = it.next();
		V value = proxy.get(key);
		if(value==null){
			return next();
		}
		else{
			lastEntry = new DummyEntry(key, value, proxy);
			return lastEntry;
		}
		
	}

	public void remove() {
		it.remove();
		proxy.remove(lastEntry.getKey(), lastEntry.getValue());
	}
	private class DummyEntry implements Entry<K,V>{

		private K key;
		private V value;
		private MapClientProxy<K, V> proxy;

		public DummyEntry(K key, V value, MapClientProxy<K, V> proxy) {
			this.key = key;
			this.value = value;
			this.proxy = proxy;
		}
		
		public K getKey() {
			// TODO Auto-generated method stub
			return key;
		}

		public V getValue() {
			// TODO Auto-generated method stub
			return value;
		}

		public V setValue(V arg0) {
			return proxy.put(key, arg0);
		}
	};
	
}