/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.IFunction;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @param <E>
 */
@SuppressWarnings("checkstyle:anoninnerlength")
class LazySet<E> extends AbstractSet<E> {
    final int size;
    final ClientMessage response;
    final IFunction<ClientMessage, E> function;

    Collection<E> backing;

    private LazySet(int size, ClientMessage response, IFunction<ClientMessage, E> function) {
        this.size = size;
        this.response = response;
        this.function = function;
    }

    static <T> Set<T> newLazySet(int size, ClientMessage response, IFunction<ClientMessage, T> function) {
        return new LazySet<T>(size, response, function);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean contains(Object o) {
        if (backing == null) {
            return super.contains(o);
        } else {
            return backing.contains(o);
        }
    }

    @Override
    public Iterator<E> iterator() {
        return new Iterator<E>() {
            Iterator<E> iterator;
            boolean iteratorInitialized;

            @Override
            public boolean hasNext() {
                init();
                return iterator.hasNext();
            }

            @Override
            public E next() {
                init();
                return iterator.next();
            }

            private void init() {
                if (iteratorInitialized) {
                    return;
                }

                if (backing == null) {
                    backing = new HashSet<E>(size);
                    for (int i = 0; i < size; i++) {
                        backing.add(function.apply(response));
                    }
                }

                iterator = backing.iterator();
                iteratorInitialized = true;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
