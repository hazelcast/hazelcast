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

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.Traverser;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.Traversers.traverseIterable;

/**
 * Group-by-frame processor. See {@link
 * WindowingProcessors#groupByFrame(com.hazelcast.jet.Distributed.Function,
 * com.hazelcast.jet.Distributed.ToLongFunction, WindowDefinition, WindowOperation)
 * groupByFrame(extractKeyF, extractEventSeqF, frameLength, frameOffset,
 * collector)} for documentation.
 *
 * @param <T> type of item
 * @param <K> type of grouping key
 * @param <F> type of the accumulated result in the frame
 */
final class GroupByFrameP<T, K, F> extends AbstractProcessor {
    final NavigableMap<Long, Map<K, F>> seqToKeyToFrame = new TreeMap<>();
    private final ToLongFunction<? super T> extractEventSeqF;
    private final Function<? super T, K> extractKeyF;
    private final Supplier<F> supplier;
    private final BiConsumer<F, ? super T> accumulator;
    private final FlatMapper<Punctuation, Object> puncFlatMapper;
    private final WindowDefinition windowDefinition;

    GroupByFrameP(
            Function<? super T, K> extractKeyF,
            ToLongFunction<? super T> extractEventSeqF,
            WindowDefinition windowDefinition,
            WindowOperation<? super T, F, ?> collector
    ) {
        this.windowDefinition = windowDefinition;
        this.extractKeyF = extractKeyF;
        this.extractEventSeqF = extractEventSeqF;
        this.supplier = collector.createAccumulatorF();
        this.accumulator = collector.accumulateItemF();
        this.puncFlatMapper = flatMapper(this::closedFrameTraverser);
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        T t = (T) item;
        long eventSeq = extractEventSeqF.applyAsLong(t);
        long frameSeq = windowDefinition.higherFrameSeq(eventSeq);
        K key = extractKeyF.apply(t);
        F frame = seqToKeyToFrame.computeIfAbsent(frameSeq, x -> new HashMap<>())
                                 .computeIfAbsent(key, x -> supplier.get());
        accumulator.accept(frame, t);
        return true;
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        return puncFlatMapper.tryProcess(punc);
    }

    private Traverser<Object> closedFrameTraverser(Punctuation punc) {
        return traverseWithRemoval(seqToKeyToFrame.headMap(punc.seq(), true).entrySet())
                .<Object>flatMap(seqAndFrame ->
                        traverseIterable(seqAndFrame.getValue().entrySet())
                                .map(e -> new Frame<>(seqAndFrame.getKey(), e.getKey(), e.getValue())))
                .append(punc);
    }

    private static <T> Traverser<T> traverseWithRemoval(Iterable<T> iterable) {
        Iterator<T> iterator = iterable.iterator();
        return () -> {
            if (!iterator.hasNext()) {
                return null;
            }
            try {
                return iterator.next();
            } finally {
                iterator.remove();
            }
        };
    }
}
