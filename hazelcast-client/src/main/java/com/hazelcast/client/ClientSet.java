package com.hazelcast.client;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Set;

public class ClientSet<E> extends AbstractCollection implements Set{

	private String name;
	private Set set;
	private MapClientProxy proxy;

	public ClientSet(Set set, MapClientProxy proxy) {
		this.set = set;
		this.proxy = proxy;
	}
	
	public Iterator iterator() {
		
		return new ClientIterator<E>(name, set.iterator(), proxy);
	}
	public int size() {
		return set.size();
	}
		
}