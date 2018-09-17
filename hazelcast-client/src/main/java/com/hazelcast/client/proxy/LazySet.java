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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @param <E>
 */
@SuppressWarnings("checkstyle:anoninnerlength")
final class LazySet<E> extends AbstractSet<E> {
    final int size;
    final ClientMessage response;
    final IFunction<ClientMessage, E> function;

    List<E> list;

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
    public Iterator<E> iterator() {
        return new Iterator<E>() {
            int visited;

            @Override
            public boolean hasNext() {
                return visited != size;
            }

            @Override
            public E next() {
                if (visited >= size) {
                    throw new NoSuchElementException();
                }

                if (list != null && visited < list.size()) {
                    return list.get(visited++);
                }

                if (list == null) {
                    list = new ArrayList<E>(size);
                }

                E element = function.apply(response);
                list.add(element);

                visited++;
                return element;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
