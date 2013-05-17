package com.hazelcast.client.util.pool;

/**
 * @mdogan 5/17/13
 */
public interface Destructor<E> {

    void destroy(E e);
}
