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

package com.hazelcast.internal.util;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Executor;

/**
 * Generic implementation of {@link InternalCompletableFuture}. Requires only
 * {@link NodeEngine}.
 */
public class SimpleCompletableFuture<T> extends AbstractCompletableFuture<T>
        implements InternalCompletableFuture<T> {

    public SimpleCompletableFuture(NodeEngine nodeEngine) {
        super(nodeEngine, nodeEngine.getLogger(SimpleCompletableFuture.class));
    }

    public SimpleCompletableFuture(Executor executor, ILogger logger) {
        super(executor, logger);
    }

    @Override
    public boolean setResult(Object result) {
        return super.setResult(result);
    }

    @Override
    public T join() {
        try {
            return get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean complete(Object value) {
        return setResult(value);
    }
}
