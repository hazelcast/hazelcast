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

import com.hazelcast.collection.CollectionProxy;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.multimap.MultiMapProxySupport;
import com.hazelcast.core.IList;
import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * @ali 1/14/13
 */
public class ObjectListProxy<E> extends MultiMapProxySupport implements CollectionProxy, IList<E> {

    public static final String COLLECTION_LIST_NAME = "hz:collection:name:list";

    final String listName;

    final Data key;

    public ObjectListProxy(String name, CollectionService service, NodeEngine nodeEngine) {
        super(COLLECTION_LIST_NAME, service, nodeEngine, nodeEngine.getConfig().getMultiMapConfig("list:"+name));
        listName = name;
        key = nodeEngine.toData(name);
    }

    public String getName() {
        return listName;
    }

    public void addItemListener(ItemListener<E> listener, boolean includeValue) {

    }

    public void removeItemListener(ItemListener<E> listener) {

    }

    public int size() {
        return 0;
    }

    public boolean isEmpty() {
        return false;
    }

    public boolean contains(Object o) {
        return false;
    }

    public Iterator<E> iterator() {
        return null;
    }

    public Object[] toArray() {
        return new Object[0];
    }

    public <T> T[] toArray(T[] a) {
        return null;
    }

    public boolean add(E e) {
        Data data = nodeEngine.toData(e);
        return putInternal(key, data);
    }

    public boolean remove(Object o) {
        return false;
    }

    public boolean containsAll(Collection<?> c) {
        return false;
    }

    public boolean addAll(Collection<? extends E> c) {
        return false;
    }

    public boolean addAll(int index, Collection<? extends E> c) {
        return false;
    }

    public boolean removeAll(Collection<?> c) {
        return false;
    }

    public boolean retainAll(Collection<?> c) {
        return false;
    }

    public void clear() {

    }

    public E get(int index) {
        return null;
    }

    public E set(int index, E element) {
        return null;
    }

    public void add(int index, E element) {

    }

    public E remove(int index) {
        return null;
    }

    public int indexOf(Object o) {
        return 0;
    }

    public int lastIndexOf(Object o) {
        return 0;
    }

    public ListIterator<E> listIterator() {
        return null;
    }

    public ListIterator<E> listIterator(int index) {
        return null;
    }

    public List<E> subList(int fromIndex, int toIndex) {
        return null;
    }

    public InstanceType getInstanceType() {
        return InstanceType.LIST;
    }

    public void destroy() {
    }

    public Object getId() {
        return null;
    }
}
