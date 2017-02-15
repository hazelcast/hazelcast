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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers.ResettableSingletonTraverser;

import javax.annotation.Nonnull;
import java.util.function.Consumer;
import java.util.function.Function;

public class TransformP<T, R> extends AbstractProcessor {

    private final Consumer<T> inputConsumer;
    private final Traverser<R> outputTraverser;
    private boolean itemDone = true;

    public TransformP(Function<Traverser<T>, Traverser<R>> transformer) {
        final ResettableSingletonTraverser<T> input = new ResettableSingletonTraverser<>();
        this.inputConsumer = input;
        this.outputTraverser = transformer.apply(input);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (itemDone) {
            inputConsumer.accept((T) item);
        }
        return itemDone = emitCooperatively(outputTraverser);
    }
}
