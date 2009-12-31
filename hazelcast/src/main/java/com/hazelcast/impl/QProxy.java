/**
 * 
 */
package com.hazelcast.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Instance.InstanceType;

interface QProxy extends IQueue {

    boolean offer(Object obj);

    boolean offer(Object obj, long timeout, TimeUnit unit) throws InterruptedException;

    void put(Object obj) throws InterruptedException;

    Object peek();

    Object poll();

    Object poll(long timeout, TimeUnit unit) throws InterruptedException;

    Object take() throws InterruptedException;

    int remainingCapacity();

    Iterator iterator();

    int size();

    void addItemListener(ItemListener listener, boolean includeValue);

    void removeItemListener(ItemListener listener);

    String getName();

    boolean remove(Object obj);

    int drainTo(Collection c);

    int drainTo(Collection c, int maxElements);

    void destroy();

    InstanceType getInstanceType();
}