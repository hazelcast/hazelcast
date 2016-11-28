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

package com.hazelcast.jet.impl;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;

import static com.hazelcast.jet.impl.DoneItem.DONE_ITEM;

/**
 * Decorator around a {@link Collection} which detects an attempt to add the {@link DoneItem#DONE_ITEM}.
 * It doesn't add it to the wrapped collection and raises its {@link #done} flag. It is an error to attempt to
 * add any elements after the {@code DONE_ITEM}.
 * <p>
 * <strong>NOTE:</strong> this collection breaks the contract of {@link #add(Object)} by refusing to add the
 * {@code DONE_ITEM} without throwing an exception. It has a very narrow use case and must not be used as a general
 * collection.
 */
final class CollectionWithDoneDetector extends AbstractCollection<Object> {
    boolean done;
    Collection<Object> wrapped;

    CollectionWithDoneDetector() {
    }

    @Override
    public Iterator<Object> iterator() {
        return wrapped.iterator();
    }

    @Override
    public int size() {
        return wrapped.size();
    }

    /**
     * Adds the supplied item to the wrapped collection unless the item is {@link DoneItem#DONE_ITEM}.
     * In that case raises the {@link #done} flag and returns {@code false}, but doesn't fail with an exception.
     * Must not be called when {@code done == true}.
     * @param o the item to add
     * @return {@code false} if the item is the {@code DONE_ITEM}; otherwise the result of {@code add(o)} called on the
     * wrapped collection
     */
    @Override
    public boolean add(Object o) {
        assert !done : "Attempt to add an item after the DONE_ITEM";
        if (o == DONE_ITEM) {
            done = true;
            return false;
        }
        return wrapped.add(o);
    }
}
