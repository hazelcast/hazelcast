package com.hazelcast.spring;

import com.hazelcast.client.spi.ClientProxy;

/**
 * @author asimarslan
 */
public class DummyProxyFactory implements com.hazelcast.client.spi.ClientProxyFactory{
    @Override
    public ClientProxy create(Object id) {
        throw new UnsupportedOperationException("not implemented yet");
    }
}
