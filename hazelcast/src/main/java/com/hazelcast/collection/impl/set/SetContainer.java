/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.set;

import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.config.SetConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.SetUtil.createHashSet;

public class SetContainer extends CollectionContainer {

    private static final int INITIAL_CAPACITY = 1000;

    private Set<CollectionItem> itemSet;
    private SetConfig config;

    public SetContainer() {
    }

    public SetContainer(String name, NodeEngine nodeEngine) {
        super(name, nodeEngine);
    }

    @Override
    public SetConfig getConfig() {
        if (config == null) {
            config = nodeEngine.getConfig().findSetConfig(name);
        }
        return config;
    }

    @Override
    public Map<Long, Data> addAll(List<Data> valueList) {
        final int size = valueList.size();
        final Map<Long, Data> map = createHashMap(size);
        List<CollectionItem> list = new ArrayList<CollectionItem>(size);
        for (Data value : valueList) {
            final long itemId = nextId();
            final CollectionItem item = new CollectionItem(itemId, value);
            if (!getCollection().contains(item)) {
                list.add(item);
                map.put(itemId, value);
            }
        }
        getCollection().addAll(list);

        return map;
    }

    @Override
    public Set<CollectionItem> getCollection() {
        if (itemSet == null) {
            if (itemMap != null && !itemMap.isEmpty()) {
                itemSet = createHashSet(itemMap.size());
                long maxItemId = Long.MIN_VALUE;
                for (CollectionItem collectionItem : itemMap.values()) {
                    if (collectionItem.getItemId() > maxItemId) {
                        maxItemId = collectionItem.getItemId();
                    }
                    itemSet.add(collectionItem);
                }
                setId(maxItemId + ID_PROMOTION_OFFSET);
                itemMap.clear();
            } else {
                itemSet = new HashSet<CollectionItem>(INITIAL_CAPACITY);
            }
            itemMap = null;
        }
        return itemSet;
    }

    @Override
    public Map<Long, CollectionItem> getMap() {
        if (itemMap == null) {
            if (itemSet != null && !itemSet.isEmpty()) {
                itemMap = createHashMap(itemSet.size());
                for (CollectionItem item : itemSet) {
                    itemMap.put(item.getItemId(), item);
                }
                itemSet.clear();
            } else {
                itemMap = new HashMap<Long, CollectionItem>(INITIAL_CAPACITY);
            }
            itemSet = null;
        }
        return itemMap;
    }

    @Override
    protected void onDestroy() {
        if (itemSet != null) {
            itemSet.clear();
        }
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.SET_CONTAINER;
    }
}
