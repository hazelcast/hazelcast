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
import com.hazelcast.transaction.TransactionException;

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

    protected CollectionItem add(int index, Data value){
        final CollectionItem item = new CollectionItem(this, nextId(), value);
        if (index < 0){
            return getList().add(item) ? item : null;
        } else {
            getList().add(index, item);
            return item;
        }
    }

    protected void addBackup(long itemId, Data value){
        final CollectionItem item = new CollectionItem(this, itemId, value);
        getMap().put(itemId, item);
    }

    protected CollectionItem get(int index){
        return getList().get(index);
    }

    protected CollectionItem remove(Data value) {
        final Iterator<CollectionItem> iterator = getList().iterator();
        while (iterator.hasNext()){
            final CollectionItem item = iterator.next();
            if (value.equals(item.getValue())){
                iterator.remove();
                return item;
            }
        }
        return null;
    }

    protected void removeBackup(long itemId) {
        getMap().remove(itemId);
    }

    protected int size() {
        return getList().size();
    }

    protected Map<Long, Data> clear() {
        Map<Long, Data> itemIdMap = new HashMap<Long, Data>(getList().size());
        for (CollectionItem item : getList()) {
            itemIdMap.put(item.getItemId(), (Data) item.getValue());
        }
        getList().clear();
        return itemIdMap;
    }

    protected void clearBackup(Set<Long> itemIdSet) {
        for (Long itemId : itemIdSet) {
            removeBackup(itemId);
        }
    }

    protected CollectionItem set(int index, long itemId, Data value){
        return getList().set(index, new CollectionItem(this, itemId, value));
    }

    protected void setBackup(long oldItemId, long itemId, Data value){
        getMap().remove(oldItemId);
        getMap().put(itemId, new CollectionItem(this, itemId, value));

    }

    protected CollectionItem remove(int index){
        return getList().remove(index);
    }

    protected int indexOf(boolean last, Data value){
        if (last){
            int index = getList().size();
            final ListIterator<CollectionItem> iterator = getList().listIterator(index);
            while (iterator.hasPrevious()){
                final CollectionItem item = iterator.previous();
                index--;
                if (value.equals(item.getValue())){
                    return index;
                }
            }
        } else {
            int index = -1;
            final Iterator<CollectionItem> iterator = getList().iterator();
            while (iterator.hasNext()){
                final CollectionItem item = iterator.next();
                index++;
                if (value.equals(item.getValue())){
                    return index;
                }
            }
        }
        return -1;
    }

    protected boolean contains(Set<Data> valueSet) {
        for (Data value : valueSet) {
            boolean contains = false;
            for (CollectionItem item : getList()) {
                if (value.equals(item.getValue())){
                    contains = true;
                    break;
                }
            }
            if (!contains){
                return false;
            }
        }
        return true;
    }

    protected Map<Long, Data> addAll(List<Data> valueList) {
        return addAll(0, valueList);
    }

    protected Map<Long, Data> addAll(int index, List<Data> valueList) {
        final int size = valueList.size();
        final Map<Long, Data> map = new HashMap<Long, Data>(size);
        List<CollectionItem> list = new ArrayList<CollectionItem>(size);
        for (Data value : valueList) {
            final long itemId = nextId();
            list.add(new CollectionItem(this, itemId, value));
            map.put(itemId, value);
        }
        getList().addAll(index, list);

        return map;
    }

    protected void addAllBackup(Map<Long, Data> valueMap) {
        Map<Long, CollectionItem> map = new HashMap<Long, CollectionItem>(valueMap.size());
        for (Map.Entry<Long, CollectionItem> entry : map.entrySet()) {
            final long itemId = entry.getKey();
            map.put(itemId, new CollectionItem(this, itemId, entry.getValue()));
        }
        getMap().putAll(map);
    }

    protected List<Data> sub(int from, int to){
        final List<CollectionItem> list;
        if (from == -1 && to == -1){
            list = getList();
        } else {
            list = getList().subList(from, to);
        }
        final ArrayList<Data> sub = new ArrayList<Data>(list.size());
        for (CollectionItem item : list) {
            sub.add((Data)item.getValue());
        }
        return sub;
    }

    protected Map<Long, Data> compareAndRemove(boolean retain, Set<Data> valueSet) {
        Map<Long, Data> itemIdMap = new HashMap<Long, Data>();
        final Iterator<CollectionItem> iterator = getList().iterator();
        while (iterator.hasNext()){
            final CollectionItem item = iterator.next();
            final boolean contains = valueSet.contains(item.getValue());
            if ( (contains && !retain) || (!contains && retain)){
                itemIdMap.put(item.getItemId(), (Data) item.getValue());
                iterator.remove();
            }
        }
        return itemIdMap;
    }

    public CollectionItem reserveRemove(long reservedItemId, Data value) {
        final Iterator<CollectionItem> iterator = getList().iterator();
        while (iterator.hasNext()){
            final CollectionItem item = iterator.next();
            if (value.equals(item.getValue())){
                iterator.remove();
                txMap.put(item.getItemId(), item);
                return item;
            }
        }
        if (reservedItemId != -1){
            return txMap.remove(reservedItemId);
        }
        return null;
    }

    public void reserveRemoveBackup(long itemId) {
        final CollectionItem item = getMap().remove(itemId);
        if (item == null) {
            throw new TransactionException("Backup reserve failed: " + itemId);
        }
        txMap.put(itemId, item);
    }

    public void rollbackRemove(long itemId) {
        final CollectionItem item = txMap.remove(itemId);
        if (item == null) {
            logger.warning("rollbackRemove No txn item for itemId: " + itemId);
        }
        getList().add(item);
    }

    public void rollbackRemoveBackup(long itemId) {
        final CollectionItem item = txMap.remove(itemId);
        if (item == null) {
            logger.warning("rollbackRemoveBackup No txn item for itemId: " + itemId);
        }
    }

    public void commitAdd(long itemId, Data value) {
        final CollectionItem item = txMap.remove(itemId);
        if (item == null) {
            throw new TransactionException("No reserve :" + itemId);
        }
        item.setValue(value);
        getList().add(item);
    }

    public void commitAddBackup(long itemId, Data value) {
        CollectionItem item = txMap.remove(itemId);
        if (item == null) {
            item = new CollectionItem(this, itemId, value);
        }
        getMap().put(itemId, item);
    }

    public void commitRemove(long itemId) {
        if (txMap.remove(itemId) == null) {
            logger.warning("commitRemove operation-> No txn item for itemId: " + itemId);
        }
    }

    public void commitRemoveBackup(long itemId) {
        if (txMap.remove(itemId) == null) {
            logger.warning("commitRemoveBackup operation-> No txn item for itemId: " + itemId);
        }
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
