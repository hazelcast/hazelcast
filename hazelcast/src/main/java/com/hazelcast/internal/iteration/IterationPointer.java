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

package com.hazelcast.internal.iteration;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

/**
 * Pointer structure representing the state of iteration in a structure.
 * The index defines the position in the structure while the size defines
 * the size of the structure and can be used to check if the structure has
 * been resized.
 * A size of {@code -1} represents an unknown size.
 * An index of {@code -1} means there are no more entries and iteration has
 * completed.
 */
public class IterationPointer {
    private int index;
    private int size;

    public IterationPointer(int index, int size) {
        this.index = index;
        this.size = size;
    }

    public IterationPointer(IterationPointer other) {
        this.index = other.index;
        this.size = other.size;
    }

    /**
     * Returns the iteration index representing the position of the iteration
     * in the backing structure.
     * An index of {@code -1} means there are no more entries and iteration has
     * completed.
     */
    public int getIndex() {
        return index;
    }

    /**
     * Sets the iteration index representing the position of the iteration in
     * the backing structure.
     * An index of {@code -1} means there are no more entries and iteration has
     * completed.
     *
     * @param index the iteration index
     */
    public void setIndex(int index) {
        this.index = index;
    }

    /**
     * Returns the size of the backing structure.
     * A size of {@code -1} represents an unknown size.
     */
    public int getSize() {
        return size;
    }

    /**
     * Sets the size of the backing structure.
     * A size of {@code -1} represents an unknown size.
     */
    public void setSize(int size) {
        this.size = size;
    }

    public static IterationPointer[] decodePointers(List<Entry<Integer, Integer>> pointers) {
        IterationPointer[] decoded = new IterationPointer[pointers.size()];
        int i = 0;
        for (Entry<Integer, Integer> pointer : pointers) {
            decoded[i++] = new IterationPointer(pointer.getKey(), pointer.getValue());
        }
        return decoded;
    }

    public static Collection<Entry<Integer, Integer>> encodePointers(IterationPointer[] pointers) {
        ArrayList<Entry<Integer, Integer>> encoded = new ArrayList<>(pointers.length);
        for (IterationPointer pointer : pointers) {
            encoded.add(new SimpleEntry<>(pointer.getIndex(), pointer.getSize()));
        }
        return encoded;
    }

    @Override
    public String toString() {
        return "IterationPointer{"
                + "index=" + index
                + ", size=" + size
                + '}';
    }
}
