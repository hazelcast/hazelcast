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

import com.hazelcast.jet2.Consumer;

import java.util.ArrayList;
import java.util.List;

public class ListConsumer<T> implements Consumer<T> {

    private final List<T> list;
    private boolean isComplete;
    private int yieldIndex = -1;

    public ListConsumer() {
        list = new ArrayList<T>();
    }

    @Override
    public boolean consume(T item) {
        if (list.size() == yieldIndex) {
            yieldIndex = -1;
            return false;
        }
        list.add(item);
        return true;
    }

    public void yieldOn(int index) {
        yieldIndex = index;
    }

    @Override
    public void complete() {
        isComplete = true;
    }

    public boolean isComplete() {
        return isComplete;
    }

    public List<T> getList() {
        return list;
    }
}
