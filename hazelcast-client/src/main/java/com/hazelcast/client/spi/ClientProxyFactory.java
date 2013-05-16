package com.hazelcast.client.spi;

/**
 * @mdogan 5/15/13
 */
public interface ClientProxyFactory {

    ClientProxy create(Object id);

}
