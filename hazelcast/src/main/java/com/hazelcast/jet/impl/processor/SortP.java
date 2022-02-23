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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.memory.AccumulationLimitExceededException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.PriorityQueue;

public class SortP<T> extends AbstractProcessor {

    private final PriorityQueue<T> priorityQueue;
    private final Traverser<T> resultTraverser;

    private long maxItems;

    public SortP(@Nullable Comparator<T> comparator) {
        this.priorityQueue = new PriorityQueue<>(comparator);
        this.resultTraverser = priorityQueue::poll;
    }

    @Override
    protected void init(@Nonnull Processor.Context context) throws Exception {
        maxItems = context.maxProcessorAccumulatedRecords();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess0(@Nonnull Object item) {
        if (priorityQueue.size() == maxItems) {
            throw new AccumulationLimitExceededException();
        }

        priorityQueue.add((T) item);
        return true;
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(resultTraverser);
    }
}
