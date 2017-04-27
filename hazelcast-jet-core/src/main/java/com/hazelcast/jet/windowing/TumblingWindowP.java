/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.windowing;

import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.Traverser;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseIterableWithRemoval;

/**
 * Tumbling window processor. See {@link
 * WindowingProcessors#slidingWindow(WindowDefinition, WindowOperation)
 * slidingWindow(windowDef, windowOperation)} for
 * documentation.
 *
 * @param <K> type of the grouping key
 * @param <F> type of the frame accumulator object
 * @param <R> type of the finished result
 */
class TumblingWindowP<K, F, R> extends FrameCombinerBaseP<K, F, R> {

    TumblingWindowP(WindowDefinition winDef, @Nonnull WindowOperation<?, F, R> winOp) {
        super(winDef, winOp);

        assert winDef.isTumbling() : TumblingWindowP.class.getSimpleName() + " used with sliding window";

        this.flatMapper = flatMapper(this::tumblingWindowTraverser);
    }

    private Traverser<Object> tumblingWindowTraverser(Punctuation punc) {
        return traverseIterableWithRemoval(seqToKeyToFrame.headMap(punc.seq(), true).entrySet())
                .<Object>flatMap(seqAndFrame ->
                        traverseIterable(seqAndFrame.getValue().entrySet())
                                .map(e -> new Frame<>(seqAndFrame.getKey(), e.getKey(), finishF.apply(e.getValue()))))
                .append(punc);
    }
}
