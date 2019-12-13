/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

    @Override
    public String toString() {
        return "IterationPointer{"
                + "index=" + index
                + ", size=" + size
                + '}';
    }
}
