package com.hazelcast.client;

import java.util.Iterator;

import com.hazelcast.core.ISet;
import com.hazelcast.impl.ClusterOperation;

public class SetClientProxy<E> extends CollectionClientProxy<E> implements ISet<E>, ClientProxy{

	public SetClientProxy(HazelcastClient client, String name) {
		super(client, name);
	}
	
	@Override	
	public boolean add(E o) {
		return (Boolean)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_ADD_TO_SET, o, null);
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
		return InstanceType.SET;
	}

	public void setOutRunnable(OutRunnable out) {
		proxyHelper.setOutRunnable(out);
	}

}
