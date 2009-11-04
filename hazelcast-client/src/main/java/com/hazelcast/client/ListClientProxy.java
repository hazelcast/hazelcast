package com.hazelcast.client;


import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IList;
import com.hazelcast.core.ItemListener;
import com.hazelcast.impl.ClusterOperation;

public class ListClientProxy<E> extends CollectionClientProxy implements IList<E>{

	private HazelcastClient client;

	public ListClientProxy(HazelcastClient hazelcastClient, String name) {
		this.client = hazelcastClient;
		this.name = name;
	}

	public void addItemListener(final ItemListener<E> listener, boolean includeValue) {
		Packet request = createRequestPacket(ClusterOperation.ADD_LISTENER, null, null);
		request.setLongValue(includeValue?1:0);
	    Call c = createCall(request);
	    client.listenerManager.addListenerCall(c);
	    doCall(c);
	    client.listenerManager.registerEntryListener(name, null, new EntryListener(){
	    	
			public void entryAdded(EntryEvent event) {
				listener.itemAdded((E)event.getKey());
			}

			public void entryEvicted(EntryEvent event) {
				// TODO Auto-generated method stub
			}

			public void entryRemoved(EntryEvent event) {
				listener.itemRemoved((E)event.getKey());
			}

			public void entryUpdated(EntryEvent event) {
				// TODO Auto-generated method stub
			}
	    	
	    });
		
	}
	
	public String getName(){
		return name.substring(4);
		
	}

	public void removeItemListener(ItemListener<E> listener) {
		// TODO Auto-generated method stub
		
	}

	public void destroy() {
		// TODO Auto-generated method stub
		
	}

	public Object getId() {
		// TODO Auto-generated method stub
		return null;
	}

	public InstanceType getInstanceType() {
		return InstanceType.LIST;
	}

	public boolean add(E o) {
		return (Boolean)doOp(ClusterOperation.CONCURRENT_MAP_ADD_TO_LIST, o, null);
	}

	public void add(int index, E element) {
		// TODO Auto-generated method stub
		
	}

	public boolean addAll(Collection<? extends E> c) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean addAll(int index, Collection<? extends E> c) {
		// TODO Auto-generated method stub
		return false;
	}

	public void clear() {
		// TODO Auto-generated method stub
		
	}

	public boolean contains(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean containsAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	public E get(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	public int indexOf(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	public Iterator<E> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	public int lastIndexOf(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	public ListIterator<E> listIterator() {
		// TODO Auto-generated method stub
		return null;
	}

	public ListIterator<E> listIterator(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean remove(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	public E remove(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean removeAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean retainAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	public E set(int index, E element) {
		// TODO Auto-generated method stub
		return null;
	}

	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	public List<E> subList(int fromIndex, int toIndex) {
		// TODO Auto-generated method stub
		return null;
	}

	public Object[] toArray() {
		// TODO Auto-generated method stub
		return null;
	}

	public <T> T[] toArray(T[] a) {
		// TODO Auto-generated method stub
		return null;
	}

}
