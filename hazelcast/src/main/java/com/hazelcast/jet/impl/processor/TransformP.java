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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;

import javax.annotation.Nonnull;

/**
 * Processor which, for each received item, emits all the items from the
 * traverser returned by the given item-to-traverser function.
 *
 * @param <T> received item type
 * @param <R> emitted item type
 */
public class TransformP<T, R> extends AbstractProcessor {
    private final FlatMapper<T, R> flatMapper;

    /**
     * Constructs a processor with the given mapping function.
     */
    public TransformP(@Nonnull FunctionEx<T, ? extends Traverser<? extends R>> mapper) {
        this.flatMapper = flatMapper(mapper);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        return flatMapper.tryProcess((T) item);
    }

    @Override
    public boolean closeIsCooperative() {
        return true;
    }
}
