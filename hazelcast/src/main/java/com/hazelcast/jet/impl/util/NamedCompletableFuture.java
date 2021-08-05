/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import com.hazelcast.logging.ILogger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;

/**
 * Extended {@link CompletableFuture} that has a name. Useful for debugging
 * if you have a complex async logic and don't know where each future we're
 * waiting for comes from.
 */
public class NamedCompletableFuture<T> extends CompletableFuture<T> {

    private final String name;

    public NamedCompletableFuture(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return super.toString() + " (" + name + ')';
    }

    public static CompletableFuture<Void> loggedAllOf(ILogger logger, String setName, CompletableFuture<?>... futures) {
        if (logger.isFineEnabled()) {
            Map<CompletableFuture<?>, String> remainingFutures = new HashMap<>();
            for (CompletableFuture<?> future : futures) {
                remainingFutures.put(future, getName(future));
            }
            Object lock = new Object();
            for (int i = 0; i < futures.length; i++) {
                CompletableFuture<?> future = futures[i];
                futures[i] = future.whenComplete((r, t) -> {
                    synchronized (lock) {
                        String removedName = remainingFutures.remove(future);
                        logFine(logger, "Future %s in set %s completed with %s, remaining=%s",
                                removedName, setName, t != null ? t : r, remainingFutures.values());
                    }
                });
            }
        }
        return allOf(futures);
    }

    private static String getName(CompletableFuture<?> f) {
        return f instanceof NamedCompletableFuture
                ? ((NamedCompletableFuture<?>) f).name
                : f.toString();
    }
}
