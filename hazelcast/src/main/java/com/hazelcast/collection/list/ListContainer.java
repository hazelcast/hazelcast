/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.collection.list;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionItem;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.util.*;

/**
 * @ali 8/30/13
 */
public class ListContainer extends CollectionContainer {

    private List<CollectionItem> itemList = null;
    private Map<Long, CollectionItem> itemMap = null;

    public ListContainer() {
    }

    public ListContainer(String name, NodeEngine nodeEngine, CollectionService service) {
        super(name, nodeEngine, service);
    }

    CollectionItem add(int index, Data value){
        final CollectionItem item = new CollectionItem(this, nextId(), value);
        if (index < 0){
            return getList().add(item) ? item : null;
        } else {
            getList().add(index, item);
            return item;
        }
    }

    void addBackup(long itemId, Data value){
        final CollectionItem item = new CollectionItem(this, itemId, value);
        getMap().put(itemId, item);
    }

    CollectionItem get(int index){
        return getList().get(index);
    }

    private List<CollectionItem> getList(){
        if(itemList == null){
            if (itemMap != null && !itemMap.isEmpty()){
                itemList = new ArrayList<CollectionItem>(itemMap.values());
                Collections.sort(itemList);
            } else {
                itemList = new ArrayList<CollectionItem>(1000);
            }
        }
        return itemList;
    }

    private Map<Long, CollectionItem> getMap(){
        if (itemMap == null){
            if (itemList != null && !itemList.isEmpty()){
                itemMap = new LinkedHashMap<Long, CollectionItem>(itemList.size());
                for (CollectionItem item : itemList) {
                    itemMap.put(item.getItemId(), item);
                }
            } else {
                itemMap = new HashMap<Long, CollectionItem>(1000);
            }
        }
        return itemMap;
    }

}
