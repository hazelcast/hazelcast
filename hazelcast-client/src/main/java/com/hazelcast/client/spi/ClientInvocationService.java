package com.hazelcast.client.spi;

import com.hazelcast.nio.Address;

/**
 * @mdogan 5/16/13
 */
public interface ClientInvocationService {

    Object invokeOnRandomTarget(Object request);

    Object invokeOnTarget(Address target, Object request);

    Object invokeOnKeyOwner(Object request, Object key);

}
