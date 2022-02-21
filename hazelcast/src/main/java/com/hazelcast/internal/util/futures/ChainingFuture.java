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


package com.hazelcast.internal.util.futures;

import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.Iterator;

/**
 * Iterates over supplied {@link InternalCompletableFuture} serially.
 * It advances to the next future only when the previous future is completed.
 *
 * It completes when there is no other future available.
 *
 * @param <T>
 */
public class ChainingFuture<T> extends InternalCompletableFuture<T> {

    private final ExceptionHandler exceptionHandler;

    public ChainingFuture(Iterator<InternalCompletableFuture<T>> futuresToChain,
                          ExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        if (!futuresToChain.hasNext()) {
            complete(null);
        } else {
            InternalCompletableFuture<T> future = futuresToChain.next();
            registerCallback(future, futuresToChain);
        }
    }

    private void registerCallback(InternalCompletableFuture<T> future,
                                  final Iterator<InternalCompletableFuture<T>> invocationIterator) {
        future.whenCompleteAsync((response, t) -> {
            if (t == null) {
                advanceOrComplete(response, invocationIterator);
            } else {
                try {
                    exceptionHandler.handle(t);
                    advanceOrComplete(null, invocationIterator);
                } catch (Throwable throwable) {
                    completeExceptionally(t);
                }
            }
        });
    }

    private void advanceOrComplete(T response, Iterator<InternalCompletableFuture<T>> invocationIterator) {
        try {
            boolean hasNext = invocationIterator.hasNext();
            if (!hasNext) {
                complete(response);
            } else {
                InternalCompletableFuture<T> future = invocationIterator.next();
                registerCallback(future, invocationIterator);
            }
        } catch (Throwable t) {
            completeExceptionally(t);
        }
    }

    public interface ExceptionHandler {
        <T extends Throwable> void handle(T throwable) throws T;
    }
}
