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

import com.hazelcast.jet.stream.impl.pipeline.TransformOperation;
import com.hazelcast.jet2.AbstractProcessor;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class TransformProcessor extends AbstractProcessor {

    private final TransformOperation[] operations;

    public TransformProcessor(List<TransformOperation> operations) {
        this.operations = operations.toArray(new TransformOperation[operations.size()]);
    }

    @Override
    protected boolean process(int ordinal, Object item) {
        processItem(item, 0);
        return true;
    }

    private void processItem(Object item, int operatorIndex) {
        for (int i = operatorIndex; i < operations.length; i++) {
            TransformOperation operation = operations[i];
            switch (operation.getType()) {
                case FILTER:
                    if (!((Predicate) operation.getFunction()).test(item)) {
                        return;
                    }
                    break;
                case MAP:
                    item = ((Function) operation.getFunction()).apply(item);
                    break;
                case FLAT_MAP:
                    Stream stream = (Stream) ((Function) operation.getFunction()).apply(item);
                    Iterator iterator = stream.iterator();
                    while (iterator.hasNext()) {
                        processItem(iterator.next(), i + 1);
                    }
                    stream.close();
                    return;
                default:
                    throw new IllegalArgumentException("Unknown case: " + operation.getType());
            }
        }
        emit(item);
    }
}
