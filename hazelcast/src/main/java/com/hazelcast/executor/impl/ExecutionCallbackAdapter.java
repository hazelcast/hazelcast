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

package com.hazelcast.executor.impl;

import com.hazelcast.core.ExecutionCallback;

import java.util.function.BiConsumer;

/**
 * Adapts an {@link com.hazelcast.core.ExecutionCallback} as a {@link java.util.function.BiConsumer}
 * so it can be used with {@link java.util.concurrent.CompletableFuture#whenComplete(BiConsumer)}.
 */
public class ExecutionCallbackAdapter<T> implements BiConsumer<T, Throwable> {

    private final ExecutionCallback<T> executionCallback;

    public ExecutionCallbackAdapter(ExecutionCallback<T> executionCallback) {
        this.executionCallback = executionCallback;
    }

    @Override
    public final void accept(T response, Throwable t) {
        if (t == null) {
            executionCallback.onResponse((T) interceptResponse(response));
        } else {
            executionCallback.onFailure(t);
        }
    }

    protected Object interceptResponse(Object o) {
        return o;
    }
}
