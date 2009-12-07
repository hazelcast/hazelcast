package com.hazelcast.client;

import java.util.Iterator;

import com.hazelcast.core.ISet;
import com.hazelcast.core.IList;
import com.hazelcast.impl.ClusterOperation;
import static com.hazelcast.client.ProxyHelper.check;

public class SetClientProxy<E> extends CollectionClientProxy<E> implements ISet<E>, ClientProxy{

	public SetClientProxy(HazelcastClient client, String name) {
		super(client, name);
	}
	
	@Override	
	public boolean add(E o) {
        check(o);
		return (Boolean)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_ADD_TO_SET, o, null);
	}
	@Override
	public boolean remove(Object o) {
        check(o);
		return (Boolean)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_REMOVE_ITEM, o, null);
	}
	@Override
	public boolean contains(Object o) {
        check(o);
		return (Boolean)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_CONTAINS, o, null);
	}


    @Override
    public boolean equals(Object o){
        if(o instanceof ISet && o!=null){
            return getName().equals(((ISet)o).getName());
        }
        else{
            return false;
        }
    }

	public String getName() {
		return name.substring(4);
	}

	public InstanceType getInstanceType() {
		return InstanceType.SET;
	}

}
