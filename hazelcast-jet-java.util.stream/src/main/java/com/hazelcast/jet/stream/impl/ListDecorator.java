/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.ItemListener;
import com.hazelcast.jet.stream.DistributedStream;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.source.ListSourcePipeline;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

@SuppressWarnings({"checkstyle:methodcount", "deprecation"})
public class ListDecorator<E> implements IStreamList<E> {

    private final IList<E> list;
    private final HazelcastInstance instance;

    public ListDecorator(IList<E> list, HazelcastInstance instance) {
        this.list = list;
        this.instance = instance;
    }

    @Override
    public String getPartitionKey() {
        return list.getPartitionKey();
    }

    @Override
    public String getName() {
        return list.getName();
    }

    @Override
    public String getServiceName() {
        return list.getServiceName();
    }

    @Override
    public void destroy() {
        list.destroy();
    }

    @Override
    public String addItemListener(ItemListener<E> itemListener, boolean b) {
        return list.addItemListener(itemListener, b);
    }

    @Override
    public boolean removeItemListener(String s) {
        return list.removeItemListener(s);
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return list.contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return list.iterator();
    }

    @Override
    public Object[] toArray() {
        return list.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return list.toArray(a);
    }

    @Override
    public boolean add(E e) {
        return list.add(e);
    }

    @Override
    public boolean remove(Object o) {
        return list.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return list.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return list.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        return list.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return list.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return list.retainAll(c);
    }

    @Override
    public void clear() {
        list.clear();
    }

    @Override
    public E get(int index) {
        return list.get(index);
    }

    @Override
    public E set(int index, E element) {
        return list.set(index, element);
    }

    @Override
    public void add(int index, E element) {
        list.add(index, element);
    }

    @Override
    public E remove(int index) {
        return list.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return list.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return list.lastIndexOf(o);
    }

    @Override
    public ListIterator<E> listIterator() {
        return list.listIterator();
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        return list.listIterator(index);
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        return list.subList(fromIndex, toIndex);
    }

    @Override
    public DistributedStream<E> stream() {
        return new ListSourcePipeline<>(new StreamContext(instance), list);
    }

    @Override
    public DistributedStream<E> parallelStream() {
        return stream().unordered();
    }
}
