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

package com.hazelcast.jet.impl.util;

import java.util.List;
import java.util.RandomAccess;

/**
 * Cursor that repeatedly traverses a list. Initially it is at the first
 * element and advances from the last element back to the first.
 *
 * @param <E> element type
 */
public class CircularListCursor<E> {
    private int index;
    private final List<E> list;

    public CircularListCursor(List<E> list) {
        assert list instanceof RandomAccess : "Attempt to create CircularListCursor with non-RandomAccess list";
        this.list = list;
    }

    /**
     * Advances to the next list element. If there is no next element, advances
     * back to the first element.
     *
     * @return {@code false} if the list is empty, {@code true} otherwise
     */
    public boolean advance() {
        if (list.isEmpty()) {
            return false;
        }
        if (!(++index < list.size())) {
            index = 0;
        }
        return true;
    }

    /**
     * Returns the item at the cursor's current position.
     */
    public E value() {
        return list.get(index);
    }

    /**
     * Positions the cursor at the first list element.
     */
    public void reset() {
        index = 0;
    }

    /**
     * Removes the current item from the underlying collection and points the cursor
     * to the previous item, wrapping around to the last item if necessary.
     */
    public void remove() {
        list.remove(index--);
        if (index < 0) {
            index = list.size() - 1;
        }
    }

    @Override
    public String toString() {
        return "CircularListCursor{index=" + index + '}';
    }
}
