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

package com.hazelcast.query.impl.collections;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

class ReadOnlyMultiCollectionIterator<E> implements Iterator<E> {

    /**
     * The chain of iterators
     */
    private final Queue<Iterator<? extends E>> iteratorChain = new LinkedList<Iterator<? extends E>>();
    /**
     * The current iterator
     */
    private Iterator<? extends E> currentIterator;

    public ReadOnlyMultiCollectionIterator(final Iterator<Map<?, ? extends E>> iterator) {
        while (iterator.hasNext()) {
            iteratorChain.add(iterator.next().values().iterator());
        }
    }

    /**
     * Updates the current iterator field to ensure that the current Iterator is
     * not exhausted
     */
    protected void updateCurrentIterator() {
        if (currentIterator == null) {
            if (iteratorChain.isEmpty()) {
                currentIterator = Collections.<E>emptySet().iterator();
            } else {
                currentIterator = iteratorChain.remove();
            }
        }

        while (!currentIterator.hasNext() && !iteratorChain.isEmpty()) {
            currentIterator = iteratorChain.remove();
        }
    }

    @Override
    public boolean hasNext() {
        updateCurrentIterator();
        return currentIterator.hasNext();
    }

    /**
     * Returns the next Object of the current Iterator
     *
     * @return Object from the current Iterator
     * @throws java.util.NoSuchElementException if all the Iterators are
     *                                          exhausted
     */
    @Override
    public E next() {
        updateCurrentIterator();
        return currentIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
