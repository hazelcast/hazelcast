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

package com.hazelcast.jet.stream.impl.processor;

import com.hazelcast.jet2.AbstractProcessor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class SortProcessor<T> extends AbstractProcessor {

    private final List<T> list;
    private final Comparator<T> comparator;
    private Iterator<T> iterator;

    public SortProcessor(Comparator<T> comparator) {
        this.list = new ArrayList<>();
        this.comparator = comparator;
    }

    @Override
    protected boolean process(int ordinal, Object item) {
        list.add((T) item);
        return true;
    }

    @Override
    public boolean complete() {
        if (iterator == null) {
            Collections.sort(list, comparator);
            iterator = list.iterator();
        }
        while (iterator.hasNext() && !getOutbox().isHighWater()) {
            T key = iterator.next();
            emit(key);
        }
        return !iterator.hasNext();
    }
}
