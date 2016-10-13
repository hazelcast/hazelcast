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

import java.util.Iterator;
import java.util.List;

public class ListProducer extends AbstractProducer {

    private Iterator<?> iterator;
    private int batchSize;
    private boolean paused;
    private boolean completeEarly;
    private boolean completed;

    public ListProducer(List<?> list, int batchSize) {
        this.iterator = list.iterator();
        this.batchSize = batchSize;
    }

    @Override
    public boolean complete() {
        if (completed) {
            throw new IllegalStateException("process() called after completion");
        }
        if (completeEarly) {
            completed = true;
            return true;
        }
        if (paused) {
            return false;
        }

        for (int i = 0; i < batchSize && iterator.hasNext(); i++) {
            emit(iterator.next());
        }
        completed = !iterator.hasNext();
        return completed;
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
}
