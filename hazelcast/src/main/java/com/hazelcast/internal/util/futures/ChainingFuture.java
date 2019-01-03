/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.AbstractCompletableFuture;

import java.util.Iterator;
import java.util.concurrent.Executor;

/**
 * Iterates over supplied {@link ICompletableFuture} serially.
 * It advances to the next future only when the previous future is completed.
 *
 * It completes when there is no other future available.
 *
 * @param <T>
 */
public class ChainingFuture<T> extends AbstractCompletableFuture<T> {

    private final ExceptionHandler exceptionHandler;

    public ChainingFuture(Iterator<ICompletableFuture<T>> futuresToChain, Executor executor,
                          ExceptionHandler exceptionHandler, ILogger logger) {
        super(executor, logger);
        this.exceptionHandler = exceptionHandler;


        if (!futuresToChain.hasNext()) {
            setResult(null);
        } else {
            ICompletableFuture<T> future = futuresToChain.next();
            registerCallback(future, futuresToChain);
        }
    }

    private void registerCallback(ICompletableFuture<T> future, final Iterator<ICompletableFuture<T>> invocationIterator) {
        future.andThen(new ExecutionCallback<T>() {
            @Override
            public void onResponse(T response) {
                advanceOrComplete(response, invocationIterator);
            }

            @Override
            public void onFailure(Throwable t) {
                try {
                    exceptionHandler.handle(t);
                    advanceOrComplete(null, invocationIterator);
                } catch (Throwable throwable) {
                    setResult(t);
                }
            }
        });
    }

    private void advanceOrComplete(T response, Iterator<ICompletableFuture<T>> invocationIterator) {
        try {
            boolean hasNext = invocationIterator.hasNext();
            if (!hasNext) {
                setResult(response);
            } else {
                ICompletableFuture<T> future = invocationIterator.next();
                registerCallback(future, invocationIterator);
            }
        } catch (Throwable t) {
            setResult(t);
        }
    }

    public interface ExceptionHandler {
        <T extends Throwable> void handle(T throwable) throws T;
    }
}
