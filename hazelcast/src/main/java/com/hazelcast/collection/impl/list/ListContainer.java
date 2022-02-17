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

package com.hazelcast.collection.impl.list;

import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.collection.impl.collection.TxCollectionItem;
import com.hazelcast.config.ListConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static java.util.Collections.sort;

public class ListContainer extends CollectionContainer {

    private static final int INITIAL_CAPACITY = 1000;
    private List<CollectionItem> itemList;
    private ListConfig config;

    public ListContainer() {
    }

    public ListContainer(String name, NodeEngine nodeEngine) {
        super(name, nodeEngine);
    }

    @Override
    public ListConfig getConfig() {
        if (config == null) {
            config = nodeEngine.getConfig().findListConfig(name);
        }
        return config;
    }

    @Override
    public void rollbackRemove(long itemId) {
        TxCollectionItem txItem = txMap.remove(itemId);
        if (txItem == null) {
            logger.warning("Transaction log cannot be found for rolling back 'remove()' operation."
                    + " Missing log item ID: " + itemId);
            return;
        }
        CollectionItem item = new CollectionItem(itemId, txItem.getValue());
        addTxItemOrdered(item);
    }

    private void addTxItemOrdered(CollectionItem item) {
        ListIterator<CollectionItem> iterator = getCollection().listIterator();
        while (iterator.hasNext()) {
            CollectionItem collectionItem = iterator.next();
            if (item.getItemId() < collectionItem.getItemId()) {
                iterator.previous();
                break;
            }
        }
        iterator.add(item);
    }

    public CollectionItem add(int index, Data value) {
        final CollectionItem item = new CollectionItem(nextId(), value);
        if (index < 0) {
            return getCollection().add(item) ? item : null;
        } else {
            getCollection().add(index, item);
            return item;
        }
    }

    public CollectionItem get(int index) {
        return getCollection().get(index);
    }

    public CollectionItem set(int index, long itemId, Data value) {
        return getCollection().set(index, new CollectionItem(itemId, value));
    }

    public void setBackup(long oldItemId, long itemId, Data value) {
        getMap().remove(oldItemId);
        getMap().put(itemId, new CollectionItem(itemId, value));

    }

    public CollectionItem remove(int index) {
        return getCollection().remove(index);
    }

    public int indexOf(boolean last, Data value) {
        final List<CollectionItem> list = getCollection();
        if (last) {
            int index = list.size();
            final ListIterator<CollectionItem> iterator = list.listIterator(index);
            while (iterator.hasPrevious()) {
                final CollectionItem item = iterator.previous();
                index--;
                if (value.equals(item.getValue())) {
                    return index;
                }
            }
        } else {
            int index = -1;
            for (CollectionItem item : list) {
                index++;
                if (value.equals(item.getValue())) {
                    return index;
                }
            }
        }
        return -1;
    }

    public Map<Long, Data> addAll(int index, List<Data> valueList) {
        final int size = valueList.size();
        final Map<Long, Data> map = createHashMap(size);
        List<CollectionItem> list = new ArrayList<CollectionItem>(size);
        for (Data value : valueList) {
            final long itemId = nextId();
            list.add(new CollectionItem(itemId, value));
            map.put(itemId, value);
        }
        getCollection().addAll(index, list);

        return map;
    }

    public List<Data> sub(int from, int to) {
        final List<CollectionItem> list;
        if (from == -1 && to == -1) {
            list = getCollection();
        } else if (to == -1) {
            List<CollectionItem> collection = getCollection();
            list = collection.subList(from, collection.size());
        } else {
            list = getCollection().subList(from, to);
        }
        final ArrayList<Data> sub = new ArrayList<Data>(list.size());
        for (CollectionItem item : list) {
            sub.add(item.getValue());
        }
        return sub;
    }

    @Override
    public List<CollectionItem> getCollection() {
        if (itemList == null) {
            if (itemMap != null && !itemMap.isEmpty()) {
                itemList = new ArrayList<CollectionItem>(itemMap.values());
                sort(itemList);
                CollectionItem lastItem = itemList.get(itemList.size() - 1);
                setId(lastItem.getItemId() + ID_PROMOTION_OFFSET);
                itemMap.clear();
            } else {
                itemList = new ArrayList<CollectionItem>(INITIAL_CAPACITY);
            }
            itemMap = null;
        }
        return itemList;
    }

    @Override
    public Map<Long, CollectionItem> getMap() {
        if (itemMap == null) {
            if (itemList != null && !itemList.isEmpty()) {
                itemMap = createHashMap(itemList.size());
                for (CollectionItem item : itemList) {
                    itemMap.put(item.getItemId(), item);
                }
                itemList.clear();
            } else {
                itemMap = new HashMap<Long, CollectionItem>(INITIAL_CAPACITY);
            }
            itemList = null;
        }
        return itemMap;
    }

    @Override
    protected void onDestroy() {
        if (itemList != null) {
            itemList.clear();
        }
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.LIST_CONTAINER;
    }
}
