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

package com.hazelcast.client.proxy;

import com.hazelcast.collection.client.*;
import com.hazelcast.core.IList;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.SerializableCollection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

/**
* @author ali 5/20/13
*/
public class ClientListProxy<E> extends AbstractClientCollectionProxy<E> implements IList<E> {

    public ClientListProxy(String serviceName, String name) {
        super(serviceName, name);
    }

    public boolean addAll(int index, Collection<? extends E> c) {
        throwExceptionIfNull(c);
        final List<Data> valueList = new ArrayList<Data>(c.size());
        for (E e : c) {
            throwExceptionIfNull(e);
            valueList.add(toData(e));
        }
        final ListAddAllRequest request = new ListAddAllRequest(getName(), valueList, index);
        final Boolean result = invoke(request);
        return result;
    }

    public E get(int index) {
        final ListGetRequest request = new ListGetRequest(getName(), index);
        return invoke(request);
    }

    public E set(int index, E element) {
        throwExceptionIfNull(element);
        final Data value = toData(element);
        final ListSetRequest request = new ListSetRequest(getName(), index, value);
        return invoke(request);
    }

    public void add(int index, E element) {
        throwExceptionIfNull(element);
        final Data value = toData(element);
        final ListAddRequest request = new ListAddRequest(getName(), value, index);
        invoke(request);
    }

    public E remove(int index) {
        final ListRemoveRequest request = new ListRemoveRequest(getName(), index);
        return invoke(request);
    }

    public int indexOf(Object o) {
        return indexOfInternal(o, false);
    }

    public int lastIndexOf(Object o) {
        return indexOfInternal(o, true);
    }

    private int indexOfInternal(Object o, boolean last){
        throwExceptionIfNull(o);
        final Data value = toData(o);
        final ListIndexOfRequest request = new ListIndexOfRequest(getName(), value, last);
        final Integer result = invoke(request);
        return result;
    }

    public ListIterator<E> listIterator() {
        return listIterator(0);
    }

    public ListIterator<E> listIterator(int index) {
        return subList(-1, -1).listIterator(index);
    }

    public List<E> subList(int fromIndex, int toIndex) {
        final ListSubRequest request = new ListSubRequest(getName(), fromIndex, toIndex);
        final SerializableCollection result = invoke(request);
        final Collection<Data> collection = result.getCollection();
        final List<E> list = new ArrayList<E>(collection.size());
        for (Data value : collection) {
            list.add((E)toObject(value));
        }
        return list;
    }

    @Override
    public String toString() {
        return "IList{" + "name='" + getName() + '\'' + '}';
    }
}
