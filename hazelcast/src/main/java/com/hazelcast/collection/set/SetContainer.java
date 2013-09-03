package com.hazelcast.collection.set;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionItem;
import com.hazelcast.nio.serialization.Data;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @ali 9/3/13
 */
public class SetContainer extends CollectionContainer {

    protected CollectionItem remove(Data value) {
        return null;
    }

    @Override
    protected void removeBackup(long itemId) {

    }

    @Override
    protected int size() {
        return 0;
    }

    @Override
    protected Map<Long, Data> clear() {
        return null;
    }

    @Override
    protected void clearBackup(Set<Long> itemIdSet) {

    }

    @Override
    protected boolean contains(Set<Data> valueSet) {
        return false;
    }

    @Override
    protected Map<Long, Data> addAll(List<Data> valueList) {
        return null;
    }

    @Override
    protected void addAllBackup(Map<Long, Data> valueMap) {

    }

    @Override
    protected Map<Long, Data> compareAndRemove(boolean retain, Set<Data> valueSet) {
        return null;
    }
}
