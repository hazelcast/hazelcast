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

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Outbox;

import javax.annotation.Nonnull;
import java.util.function.BiFunction;

public class AccumulatorProcessor<IN, OUT> extends AbstractProcessor {

    private final BiFunction<OUT, IN, OUT> accumulator;
    private final OUT identity;
    private OUT result;


    public AccumulatorProcessor(BiFunction<OUT, IN, OUT> accumulator, OUT identity) {
        this.accumulator = accumulator;
        this.identity = identity;
    }

    @Override
    public void init(@Nonnull Outbox outbox) {
        super.init(outbox);
        result = identity;
    }

    @Override
    protected boolean process(int ordinal, Object item) {
        result = accumulator.apply(result, (IN) item);
        return true;
    }

    @Override
    public boolean complete() {
        emit(result);
        return true;
    }
}
