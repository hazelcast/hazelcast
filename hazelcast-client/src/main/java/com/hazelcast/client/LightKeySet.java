package com.hazelcast.client;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Set;

public class LightKeySet<E> extends AbstractCollection<E> implements Set<E>{
	final Set<E> realSet;
	final MapClientProxy<?,?> proxy;
	public LightKeySet(MapClientProxy<?,?> proxy, Set<E> set) {
		this.proxy = proxy;
		this.realSet = set;
	}

	@Override
	public Iterator<E> iterator() {
		final Iterator<E> iterator = realSet.iterator();
		return new Iterator<E>(){
			volatile E lastEntry;

			public boolean hasNext() {
				return iterator.hasNext();
			}

			public E next() {
				lastEntry = iterator.next();
				return lastEntry;
			}

			public void remove() {
				iterator.remove();
				proxy.remove(lastEntry);
			}
		};
	}
	
	@Override
	public int size() {
		return realSet.size();
	}
}