package com.hazelcast.client;


import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.hazelcast.core.IList;
import com.hazelcast.impl.ClusterOperation;

public class ListClientProxy<E> extends CollectionClientProxy<E> implements IList<E>, ClientProxy{
	public ListClientProxy(HazelcastClient hazelcastClient, String name) {
		super(hazelcastClient, name);
	}


	@Override	
	public boolean add(E o) {
		return (Boolean)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_ADD_TO_LIST, o, null);
	}
	@Override
	public boolean remove(Object o) {
		return (Boolean)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_REMOVE_ITEM, o, null);
	}
	@Override
	public boolean contains(Object o) {
		return (Boolean)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_CONTAINS, o, null);
	}



	public String getName() {
		return name.substring(4);
	}

	public void destroy() {
		proxyHelper.destroy();
	}

	public Object getId() {
		return name;
	}

	public InstanceType getInstanceType() {
		return InstanceType.LIST;
	}

	public void add(int index, E element) {
		throw new UnsupportedOperationException();
	}

	public boolean addAll(int index, Collection<? extends E> c) {
		Boolean result = true;
		for (Iterator<? extends E> iterator = c.iterator(); iterator.hasNext();) {
			E e = (E) iterator.next();
			if(!add(e)){
				result = false;
			}
		}
		return result;
	}

	public E get(int index) {
		throw new UnsupportedOperationException();
	}

	public int indexOf(Object o) {
		throw new UnsupportedOperationException();
	}

	public int lastIndexOf(Object o) {
		throw new UnsupportedOperationException();
	}

	public ListIterator<E> listIterator() {
		throw new UnsupportedOperationException();
	}

	public ListIterator<E> listIterator(int index) {
		throw new UnsupportedOperationException();
	}

	public E remove(int index) {
		throw new UnsupportedOperationException();
	}

	public E set(int index, E element) {
		throw new UnsupportedOperationException();
	}

	public List<E> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException();
	}

	public void setOutRunnable(OutRunnable out) {
		proxyHelper.setOutRunnable(out);
	}

	
}
