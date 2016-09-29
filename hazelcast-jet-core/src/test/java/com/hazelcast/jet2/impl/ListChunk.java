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

package com.hazelcast.jet2.impl;

import com.hazelcast.jet2.Chunk;
import com.hazelcast.jet2.Cursor;

import java.util.List;

public class ListChunk<T> implements Chunk<T> {

    private final List<T> list;
    private final Cursor<T> cursor;

    public ListChunk(List<T> list) {
        this.list = list;
        this.cursor = new ListCursor<>(list);
    }

    @Override
    public Cursor<T> cursor() {
        return cursor;
    }

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    @Override
    public String toString() {
        return "ListChunk{" +
                "list=" + list +
                '}';
    }
}
