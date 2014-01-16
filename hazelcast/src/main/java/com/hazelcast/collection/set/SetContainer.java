/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.set;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionItem;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.config.SetConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.util.*;

/**
 * @ali 9/3/13
 */
public class SetContainer extends CollectionContainer {

    private Set<CollectionItem> itemSet = null;
    private SetConfig config;

    public SetContainer() {
    }

    public SetContainer(String name, NodeEngine nodeEngine, CollectionService service) {
        super(name, nodeEngine, service);
    }

    @Override
    protected SetConfig getConfig() {
        if (config == null){
            config = nodeEngine.getConfig().findSetConfig(name);
        }
        return config;
    }

    @Override
    protected Map<Long, Data> addAll(List<Data> valueList) {
        final int size = valueList.size();
        final Map<Long, Data> map = new HashMap<Long, Data>(size);
        List<CollectionItem> list = new ArrayList<CollectionItem>(size);
        for (Data value : valueList) {
            final long itemId = nextId();
            final CollectionItem item = new CollectionItem(itemId, value);
            if (!getCollection().contains(item)){
                list.add(item);
                map.put(itemId, value);
            }
        }
        getCollection().addAll(list);

        return map;
    }

    @Override
    public Set<CollectionItem> getCollection(){
        if(itemSet == null){
            if (itemMap != null && !itemMap.isEmpty()){
                itemSet = new HashSet<CollectionItem>(itemMap.values());
                itemMap.clear();
            } else {
                itemSet = new HashSet<CollectionItem>(1000);
            }
            itemMap = null;
        }
        return itemSet;
    }

    @Override
    protected Map<Long, CollectionItem> getMap(){
        if (itemMap == null){
            if (itemSet != null && !itemSet.isEmpty()){
                itemMap = new HashMap<Long, CollectionItem>(itemSet.size());
                for (CollectionItem item : itemSet) {
                    itemMap.put(item.getItemId(), item);
                }
                itemSet.clear();
            } else {
                itemMap = new HashMap<Long, CollectionItem>(1000);
            }
            itemSet = null;
        }
        return itemMap;
    }

    @Override
    protected void onDestroy() {
        if (itemSet != null){
            itemSet.clear();
        }
    }
}
