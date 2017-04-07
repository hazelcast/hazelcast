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

package com.hazelcast.jet.stream.impl.processor;

import com.hazelcast.jet.AbstractProcessor;

import javax.annotation.Nonnull;
import java.util.function.BinaryOperator;
import java.util.function.Function;

public class CombineP<T, R> extends AbstractProcessor {

    private final BinaryOperator<T> combiner;
    private final Function<T, R> finisher;
    private T result;

    public CombineP(BinaryOperator<T> combiner, Function<T, R> finisher) {
        this.combiner = combiner;
        this.finisher = finisher;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        if (result != null) {
            result = combiner.apply(result, (T) item);
        } else {
            result = (T) item;
        }
        return true;
    }

    @Override
    public boolean complete() {
        return result == null || tryEmit(finisher.apply(result));
    }
}
