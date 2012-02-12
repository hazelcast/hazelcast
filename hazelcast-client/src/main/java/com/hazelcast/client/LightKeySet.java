/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.client;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Set;

public class LightKeySet<E> extends AbstractCollection<E> implements Set<E> {
    final Set<E> realSet;
    final EntryHolder<?, ?> proxy;

    public LightKeySet(EntryHolder<?, ?> proxy, Set<E> set) {
        this.proxy = proxy;
        this.realSet = set;
    }

    @Override
    public Iterator<E> iterator() {
        final Iterator<E> iterator = realSet.iterator();
        return new Iterator<E>() {
            volatile E lastEntry;

            public boolean hasNext() {
                return iterator.hasNext();
            }

            public E next() {
                lastEntry = iterator.next();
                return lastEntry;
            }

            public void remove() {
                iterator.remove();
                proxy.remove(lastEntry);
            }
        };
    }

    @Override
    public int size() {
        return realSet.size();
    }
}
