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

import com.hazelcast.jet2.OutputCollector;
import com.hazelcast.jet2.Producer;

import java.util.Iterator;
import java.util.List;

public class ListProducer<T> implements Producer<T> {

    private Iterator<T> iterator;
    private int batchSize;
    private boolean paused;
    private boolean completeEarly;
    private boolean completed;

    public ListProducer(List<T> list) {
        this.iterator = list.iterator();
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
    }

    public void completeEarly() {
        completeEarly = true;
    }

    @Override
    public boolean produce(OutputCollector<? super T> collector) {
        if (completed) {
            throw new IllegalStateException("produce() called after completion");
        }
        if (completeEarly) {
            completed = true;
            return true;
        }
        if (paused) {
            return false;
        }

        for (int i = 0; i < batchSize && iterator.hasNext(); i++) {
            collector.collect(iterator.next());
        }
        completed = !iterator.hasNext();
        return completed;
    }
}
