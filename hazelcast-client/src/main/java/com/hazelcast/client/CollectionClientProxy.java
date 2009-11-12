package com.hazelcast.client;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;

import com.hazelcast.core.ItemListener;
import com.hazelcast.impl.ClusterOperation;

public abstract class CollectionClientProxy<E> extends AbstractCollection<E>{
	final protected HazelcastClient client;
	final protected ProxyHelper proxyHelper;
	final protected String name;
	
	public CollectionClientProxy(HazelcastClient hazelcastClient, String name) {
		this.name = name;
		this.client = hazelcastClient;
		proxyHelper = new ProxyHelper(name, hazelcastClient);
	}

    public void destroy() {
		proxyHelper.destroy();
	}

    public Object getId() {
		return name;
	}
	
	@Override
	public Iterator<E> iterator() {
		final Collection<E> collection = proxyHelper.keys(null);
		final AbstractCollection<E> proxy = this;
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
	
	public void addItemListener(ItemListener<E> listener, boolean includeValue) {
		Packet request = proxyHelper.createRequestPacket(ClusterOperation.ADD_LISTENER, null, null);
		request.setLongValue(includeValue?1:0);
	    Call c = proxyHelper.createCall(request);
	    client.listenerManager.addListenerCall(c, name, null);

	    proxyHelper.doCall(c);
	    client.listenerManager.registerItemListener(name, listener); 
	}
	
	public void removeItemListener(ItemListener<E> listener) {
		client.listenerManager.removeItemListener(name, listener);
	}
	
	@Override
	public int size() {
		return (Integer)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_SIZE, null, null);
	}

    public void setOutRunnable(OutRunnable out) {
		proxyHelper.setOutRunnable(out);
	}

    @Override
    public int hashCode(){
        return name.hashCode();
    }
    

}
