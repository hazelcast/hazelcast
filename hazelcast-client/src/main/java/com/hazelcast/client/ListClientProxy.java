package com.hazelcast.client;


import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.hazelcast.core.IList;
import com.hazelcast.core.ItemListener;
import com.hazelcast.impl.ClusterOperation;

public class ListClientProxy<E> extends CollectionClientProxy<E> implements IList<E>, ClientProxy{

	final private HazelcastClient client;
	final ProxyHelper proxyHelper;
	final private String name;

	public ListClientProxy(HazelcastClient hazelcastClient, String name) {
		this.name = name;
		this.client = hazelcastClient;
		proxyHelper = new ProxyHelper(name, hazelcastClient);
	}

	@Override
	public Iterator<E> iterator() {
		final Collection<E> collection = proxyHelper.keys(null);
		final ListClientProxy<E> proxy = this;
		return new Iterator<E>(){
			Iterator<E> iterator = collection.iterator();
			volatile E lastRecord;
			public boolean hasNext() {
				return iterator.hasNext();
			}

			public E next() {
				lastRecord = iterator.next();
				return lastRecord;
			}

			public void remove() {
				iterator.remove();
				proxy.remove(lastRecord);
			}
			
		};
	}

	@Override
	public int size() {
		return (Integer)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_SIZE, null, null);
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

	public void addItemListener(ItemListener<E> listener, boolean includeValue) {
		Packet request = proxyHelper.createRequestPacket(ClusterOperation.ADD_LISTENER, null, null);
		request.setLongValue(includeValue?1:0);
	    Call c = proxyHelper.createCall(request);
	    client.listenerManager.addListenerCall(c, name, null);

	    proxyHelper.doCall(c);
	    client.listenerManager.registerItemListener(name, listener); 
	}

	public String getName() {
		return name.substring(4);
	}

	public void removeItemListener(ItemListener<E> listener) {
		client.listenerManager.removeItemListener(name, listener);
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
