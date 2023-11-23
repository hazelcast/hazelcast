/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.impl.util.AutoCloseableTraverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * Same as {@link TransformBatchedP}. This processor will close the internal traverser
 */
public class TransformBatchedAutoCloseableP<T, R> extends AbstractProcessor {

    private final Function<? super Iterable<T>, ? extends AutoCloseableTraverser<? extends R>> mapper;

    private AutoCloseableTraverser<? extends R> outputTraverser;

    private boolean isCooperative = true;

    public TransformBatchedAutoCloseableP(Function<? super Iterable<T>, ? extends AutoCloseableTraverser<? extends R>> mapper) {
        this.mapper = mapper;
    }

    @Override
    public boolean isCooperative() {
        return isCooperative;
    }

    public TransformBatchedAutoCloseableP<T, R> setCooperative(boolean cooperative) {
        isCooperative = cooperative;
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (outputTraverser == null) {
            outputTraverser = mapper.apply((Iterable<T>) inbox);
        }

        if (emitFromTraverser(outputTraverser)) {
            inbox.clear();
            closeTraverser();
        }
    }

    @Override
    public void close() throws Exception {
        closeTraverser();
    }

    @Override
    public boolean closeIsCooperative() {
        return true;
    }

    private void closeTraverser() {
        if (outputTraverser != null) {
            try {
                outputTraverser.close();
            } catch (Exception ignored) {
            }
            outputTraverser = null;
        }
    }

}
