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

package com.hazelcast.jet.pipeline.test;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;


/**
 * Implements a mock batch source with parallelism. It emits the items from
 * the lists you supply, preserving the order within each list.
 *
 * @param <T> type of emitted items
 */
public class ParallelBatchP<T> extends AbstractProcessor {

    private List<Traverser<T>> traversers;

    private final List<? extends Iterable<T>> iterables;

    /**
     * Creates a processor that generates items using its assigned
     * iterables. This processor picks its assigned iterables from
     * the list of iterables according to its global processor index.
     * @param iterables list of iterables used to emit items
     */
    public ParallelBatchP(List<? extends Iterable<T>> iterables) {
        this.iterables = iterables;
    }

    @Override
    protected void init(@Nonnull Context context) {
        int globalProcessorIndex = context.globalProcessorIndex();
        int totalParallelism = context.totalParallelism();
        traversers = IntStream.range(0, iterables.size())
                .filter(i -> i % totalParallelism == globalProcessorIndex)
                .mapToObj(iterables::get)
                .map(Traversers::traverseIterable)
                .collect(toList());
    }

    @Override
    public boolean complete() {
        boolean isCompleted = true;
        for (Traverser<T> traverser : traversers) {
            if (!emitFromTraverser(traverser)) {
                isCompleted = false;
            }
        }
        return isCompleted;
    }
}
