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

package com.hazelcast.jet.impl.util;

import com.hazelcast.logging.ILogger;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;

/**
 * Wraps a CompletableFuture that is expected to be completed
 * only in a single way and provides methods for ease of use
 */
public class CompletionToken {
    private final CompletableFuture<Void> future = new CompletableFuture<>();
    private final ILogger logger;

    public CompletionToken(ILogger logger) {
        this.logger = logger;
    }

    public boolean complete() {
        return future.complete(null);
    }

    public boolean isCompleted() {
        return future.isDone();
    }

    public void whenCompleted(Runnable runnable) {
        future.whenComplete(withTryCatch(logger, (result, throwable) -> runnable.run()));
    }
}
