package com.hazelcast.collection.set;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionItem;
import com.hazelcast.nio.serialization.Data;

import java.util.*;

/**
 * @ali 9/3/13
 */
public class SetContainer extends CollectionContainer {

    private Set<CollectionItem> itemSet = null;
    private Map<Long, CollectionItem> itemMap = null;

    protected Map<Long, Data> addAll(List<Data> valueList) {
        final int size = valueList.size();
        final Map<Long, Data> map = new HashMap<Long, Data>(size);
        List<CollectionItem> list = new ArrayList<CollectionItem>(size);
        for (Data value : valueList) {
            final long itemId = nextId();
            final CollectionItem item = new CollectionItem(this, itemId, value);
            if (!getCollection().contains(item)){
                list.add(item);
                map.put(itemId, value);
            }
        }
        getCollection().addAll(list);

        return map;
    }

    protected Set<CollectionItem> getCollection(){
        if(itemSet == null){
            if (itemMap != null && !itemMap.isEmpty()){
                itemSet = new HashSet<CollectionItem>(itemMap.values());
            } else {
                itemSet = new HashSet<CollectionItem>(1000);
            }
        }
        return itemSet;
    }

    protected Map<Long, CollectionItem> getMap(){
        if (itemMap == null){
            if (itemSet != null && !itemSet.isEmpty()){
                itemMap = new HashMap<Long, CollectionItem>(itemSet.size());
                for (CollectionItem item : itemSet) {
                    itemMap.put(item.getItemId(), item);
                }
            } else {
                itemMap = new HashMap<Long, CollectionItem>(1000);
            }
        }
        return itemMap;
    }
}
