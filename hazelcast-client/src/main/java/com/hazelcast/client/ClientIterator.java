package com.hazelcast.client;

import java.util.Iterator;

public class ClientIterator<E> implements Iterator<E>{
	private Iterator<?> it;
	private String name;
	private MapClientProxy proxy;

	public ClientIterator(String name, Iterator<?> it, MapClientProxy proxy) {
		this.name = name;
		this.it = it;
		this.proxy = proxy;
	}

	public boolean hasNext() {
		return it.hasNext();
	}

	public E next() {
		byte[] key = (byte[])it.next();
		proxy.get(key);
		return null;
	}

	public void remove() {
		// TODO Auto-generated method stub
		
	}
	
}