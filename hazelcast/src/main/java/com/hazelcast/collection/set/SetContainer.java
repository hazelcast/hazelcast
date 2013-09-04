package com.hazelcast.collection.set;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionItem;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @ali 9/3/13
 */
public class SetContainer extends CollectionContainer {

    private Set<CollectionItem> itemSet = null;
    private Map<Long, CollectionItem> itemMap = null;

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
