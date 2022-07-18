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

package com.hazelcast.tpc.engine.iouring;

public final class Slots<E> {

    private int[] tags;
    private E[] items;
    private int pos;

    public Slots(int size) {
        tags = new int[size];
        items = (E[]) new Object[size];

        for (int k = 0; k < size; k++) {
            tags[k] = k;
        }
        pos = size;
    }

    public int insert(E item) {
        if (pos == 0) {
            throw new RuntimeException();
        }

        pos--;
        int tag = tags[pos];
        items[tag] = item;
        return tag;
    }

    public E remove(int tag) {
        E item = items[tag];
        items[tag] = null;
        tags[pos] = tag;
        pos++;
        return item;
    }
}
