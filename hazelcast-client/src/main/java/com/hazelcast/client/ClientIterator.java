package com.hazelcast.client;

import java.util.Iterator;
import java.util.Map.Entry;

public class ClientIterator<K, V> implements Iterator<java.util.Map.Entry<K, V>>{
	private Iterator<K> it;
	private String name;
	private MapClientProxy<K,V> proxy;

	public ClientIterator(String name, Iterator<K> it, MapClientProxy<K,V> proxy) {
		this.name = name;
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
			return new DummyEntry(key, value, proxy);
		}
	}

	public void remove() {
		// TODO Auto-generated method stub
		
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