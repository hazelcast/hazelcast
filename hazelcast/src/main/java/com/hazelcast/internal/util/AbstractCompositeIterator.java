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

package com.hazelcast.internal.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class AbstractCompositeIterator<E> implements Iterator<E> {

    private Iterator<E> currentIterator;
    private boolean initialized;

    @Override
    public boolean hasNext() {
        if (!initialized) {
            currentIterator = nextIterator();

            initialized = true;
        }

        return currentIterator != null;
    }

    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        E entry = currentIterator.next();

        if (!currentIterator.hasNext()) {
            currentIterator = nextIterator();
        }

        return entry;
    }

    /**
     * Return the next inner iterator or {@code null}. The returned iterator
     * MUST NOT be empty.
     */
    // TODO remove this ill-defined class after IMDG SQL engine is removed.
    protected abstract Iterator<E> nextIterator();
}
