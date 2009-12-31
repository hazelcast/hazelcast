/**
 * 
 */
package com.hazelcast.impl;

import java.util.Set;

import com.hazelcast.core.IMap;
import com.hazelcast.core.Instance.InstanceType;

public interface MProxy extends IMap, IRemoveAwareProxy, IGetAwareProxy {
    String getLongName();

    void addGenericListener(Object listener, Object key, boolean includeValue, InstanceType instanceType);

    void removeGenericListener(Object listener, Object key);

    boolean containsEntry(Object key, Object value);

    boolean putMulti(Object key, Object value);

    boolean removeMulti(Object key, Object value);

    boolean add(Object value);

    int valueCount(Object key);

    Set allKeys();
}