/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.connector.map.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;

import java.util.concurrent.CompletableFuture;

final class Hz3ImplUtil {

    private Hz3ImplUtil() {
    }

    /**
     * Converts {@link ICompletableFuture} to {@link CompletableFuture}
     */
    public static <T> CompletableFuture<T> toCompletableFuture(ICompletableFuture<T> future) {
        CompletableFuture<T> result = new CompletableFuture<>();
        future.andThen(new ExecutionCallback<T>() {
            @Override
            public void onResponse(T response) {
                result.complete(response);
            }

            @Override
            public void onFailure(Throwable t) {
                result.completeExceptionally(t);
            }
        });
        return result;
    }
}
