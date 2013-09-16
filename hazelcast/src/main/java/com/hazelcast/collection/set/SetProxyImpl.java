package com.hazelcast.collection.set;

import com.hazelcast.collection.AbstractCollectionProxyImpl;
import com.hazelcast.core.ISet;
import com.hazelcast.spi.NodeEngine;

/**
 * @ali 9/3/13
 */
public class SetProxyImpl<E> extends AbstractCollectionProxyImpl<SetService, E> implements ISet<E> {

    public SetProxyImpl(String name, NodeEngine nodeEngine, SetService service) {
        super(name, nodeEngine, service);
    }

    public String getServiceName() {
        return SetService.SERVICE_NAME;
    }

}
