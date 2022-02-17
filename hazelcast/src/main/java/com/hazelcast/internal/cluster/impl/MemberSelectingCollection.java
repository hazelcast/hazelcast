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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An immutable collection that applies all the {@link MemberSelector} instances to
 * its internal {@link Member} collection. It reflects changes in the internal collection.
 * Mutating methods throw {@link java.lang.UnsupportedOperationException}
 * It is mainly used for querying a member list.
 *
 * @param <M> A subclass of {@link Member} interface
 */
public final class MemberSelectingCollection<M extends Member> implements Collection<M> {

    private final Collection<M> members;

    private final MemberSelector selector;

    public MemberSelectingCollection(Collection<M> members, MemberSelector selector) {
        this.members = members;
        this.selector = selector;
    }

    @Override
    public int size() {
        return count(members, selector);
    }

    public static <M extends Member> int count(Collection<M> members, MemberSelector memberSelector) {
        int size = 0;
        for (M member : members) {
            if (memberSelector.select(member)) {
                size++;
            }
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        return !iterator().hasNext();
    }

    @Override
    public boolean contains(Object o) {
        for (M member : members) {
            if (selector.select(member) && o.equals(member)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Iterator<M> iterator() {
        return new MemberSelectingIterator();
    }

    @Override
    public Object[] toArray() {
        List<Object> result = new ArrayList<>();
        for (M member : members) {
            if (selector.select(member)) {
                result.add(member);
            }
        }
        return result.toArray(new Object[0]);
    }

    @Override
    public <T> T[] toArray(T[] a) {
        List<Object> result = new ArrayList<>();
        for (M member : members) {
            if (selector.select(member)) {
                result.add(member);
            }
        }

        if (a.length != result.size()) {
            a = (T[]) Array.newInstance(a.getClass().getComponentType(), result.size());
        }

        for (int i = 0; i < a.length; i++) {
            a[i] = (T) result.get(i);
        }

        return a;
    }

    @Override
    public boolean add(M member) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean addAll(Collection<? extends M> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    class MemberSelectingIterator
            implements Iterator<M> {

        private final Iterator<M> iterator = MemberSelectingCollection.this.members.iterator();

        private M member;

        @Override
        public boolean hasNext() {
            while (this.member == null && iterator.hasNext()) {
                M nextMember = iterator.next();
                if (selector.select(nextMember)) {
                    this.member = nextMember;
                }
            }

            return member != null;
        }

        @Override
        public M next() {
            M nextMember;
            if (member != null || hasNext()) {
                nextMember = member;
                member = null;
            } else {
                throw new NoSuchElementException();
            }

            return nextMember;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

}
