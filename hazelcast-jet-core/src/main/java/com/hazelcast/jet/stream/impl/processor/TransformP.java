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
import com.hazelcast.jet.stream.impl.pipeline.TransformOperation;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;

public class TransformP extends AbstractProcessor {

    private final Consumer<Object> inputConsumer;
    private final Traverser<?> outputTraverser;
    private boolean itemDone = true;

    public TransformP(List<TransformOperation> transformOps) {
        final ResettableSingletonTraverser<Object> input = new ResettableSingletonTraverser<>();
        this.inputConsumer = input;
        this.outputTraverser = withOpsApplied(input, transformOps);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (itemDone) {
            inputConsumer.accept(item);
        }
        return itemDone = emitCooperatively(outputTraverser);
    }

    private static Traverser<?> withOpsApplied(Traverser<?> input, List<TransformOperation> transformOps) {
        Traverser<?> composed = input;
        for (TransformOperation op : transformOps) {
            switch (op.getType()) {
                case FILTER:
                    composed = composed.filter((Predicate) op.getFunction());
                    break;
                case MAP:
                    composed = composed.map((Function) op.getFunction());
                    break;
                case FLAT_MAP:
                    composed = composed.flatMap(item -> traverseStream(
                            ((Function<Object, Stream<?>>) op.getFunction()).apply(item)));
                    break;
                default:
                    throw new IllegalArgumentException("Unknown case: " + op.getType());
            }
        }
        return composed;
    }
}
