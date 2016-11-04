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

package com.hazelcast.jet2.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.AbstractCompletableFuture;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BasicCompletableFuture<V> extends AbstractCompletableFuture<V> {
    public BasicCompletableFuture(NodeEngine nodeEngine, ILogger logger) {
        super(nodeEngine, logger);
    }

    @Override
    public void setResult(Object result) {
        super.setResult(result);
    }

    public static <V> ICompletableFuture<List<V>> allOf(NodeEngine nodeEngine, ILogger logger,
                                                        Collection<ICompletableFuture<V>> futures) {
        V[] results = (V[]) new Object[futures.size()];
        ICompletableFuture<V>[] futureArray = futures.toArray(new ICompletableFuture[futures.size()]);

        BasicCompletableFuture<List<V>> compositeFuture = new BasicCompletableFuture<>(nodeEngine, logger);
        AtomicInteger latch = new AtomicInteger(futures.size());
        for (int i = 0; i < futureArray.length; i++) {
            final int futureIndex = i;
            ICompletableFuture<V> future = futureArray[futureIndex];
            future.andThen(new ExecutionCallback<V>() {
                @Override
                public void onResponse(V response) {
                    results[futureIndex] = response;
                    if (latch.decrementAndGet() == 0) {
                        compositeFuture.setResult(Arrays.asList(results));
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    compositeFuture.setResult(t);
                }
            });
        }
        return compositeFuture;
    }
}
