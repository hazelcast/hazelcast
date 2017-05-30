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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.PunctuationPolicy;
import com.hazelcast.jet.ResettableSingletonTraverser;
import com.hazelcast.jet.Traverser;

import javax.annotation.Nonnull;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.Traversers.empty;

/**
 * A processor that inserts punctuation into a data stream. See
 * {@link com.hazelcast.jet.processor.Processors#insertPunctuation(
 *      com.hazelcast.jet.function.DistributedToLongFunction,
 *      com.hazelcast.jet.function.DistributedSupplier)
 * Processors.insertPunctuation()}.
 *
 * @param <T> type of the stream item
 */
public class InsertPunctuationP<T> extends AbstractProcessor {

    private final ToLongFunction<T> getTimestampF;
    private final PunctuationPolicy punctuationPolicy;
    private final ResettableSingletonTraverser<Object> singletonTraverser;
    private final FlatMapper<Object, Object> flatMapper;

    private long currPunc = Long.MIN_VALUE;

    /**
     * @param getTimestampF function that extracts the timestamp from the item
     * @param punctuationPolicy the punctuation policy
     */
    public InsertPunctuationP(@Nonnull ToLongFunction<T> getTimestampF,
                       @Nonnull PunctuationPolicy punctuationPolicy
    ) {
        this.getTimestampF = getTimestampF;
        this.punctuationPolicy = punctuationPolicy;
        this.flatMapper = flatMapper(this::traverser);
        this.singletonTraverser = new ResettableSingletonTraverser<>();
    }

    @Override
    public boolean tryProcess() {
        long newPunc = punctuationPolicy.getCurrentPunctuation();
        if (newPunc <= currPunc) {
            return true;
        }
        boolean didEmit = tryEmit(new Punctuation(newPunc));
        if (didEmit) {
            currPunc = newPunc;
        }
        return didEmit;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        return flatMapper.tryProcess(item);
    }

    private Traverser<Object> traverser(Object item) {
        long timestamp = getTimestampF.applyAsLong((T) item);
        if (timestamp < currPunc) {
            // drop late event
            return empty();
        }
        long newPunc = punctuationPolicy.reportEvent(timestamp);
        singletonTraverser.accept(item);
        if (newPunc > currPunc) {
            currPunc = newPunc;
            return singletonTraverser.prepend(new Punctuation(currPunc));
        }
        return singletonTraverser;
    }
}
