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

import com.hazelcast.jet.data.JetPair;
import com.hazelcast.jet.data.io.InputChunk;
import com.hazelcast.jet.data.io.OutputCollector;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.processor.Processor;
import com.hazelcast.jet.processor.ProcessorContext;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BinaryOperator;

public class MergeProcessor<K, V> implements Processor<Pair<K, V>, Pair<K, V>> {

    private final BinaryOperator<V> merger;
    private final Map<K, V> cache = new HashMap<>();
    private Iterator<Map.Entry<K, V>> finalizationIterator;

    public MergeProcessor(BinaryOperator<V> merger) {
        this.merger = merger;
    }

    @Override
    public boolean process(InputChunk<Pair<K, V>> inputChunk,
                           OutputCollector<Pair<K, V>> output,
                           String sourceName, ProcessorContext context) throws Exception {
        for (Pair<K, V> input : inputChunk) {
            V value = cache.get(input.getKey());
            if (value == null) {
                cache.put(input.getKey(), input.getValue());
            } else {
                cache.put(input.getKey(), merger.apply(value, input.getValue()));
            }
        }
        return true;
    }

    @Override
    public boolean complete(OutputCollector<Pair<K, V>> output,
                            ProcessorContext processorContext) throws Exception {
        boolean finalized = false;
        try {
            if (finalizationIterator == null) {
                finalizationIterator = cache.entrySet().iterator();
            }

            int idx = 0;
            while (this.finalizationIterator.hasNext()) {
                Map.Entry<K, V> next = finalizationIterator.next();
                output.collect(new JetPair<>(next.getKey(), next.getValue()));
                if (idx == processorContext.getConfig().getChunkSize() - 1) {
                    break;
                }
                idx++;
            }
            finalized = !finalizationIterator.hasNext();
        } finally {
            if (finalized) {
                finalizationIterator = null;
                cache.clear();
            }
        }
        return finalized;
    }
}
