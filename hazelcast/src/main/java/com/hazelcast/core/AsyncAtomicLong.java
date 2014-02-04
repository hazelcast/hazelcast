/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.spi.annotation.Beta;

/**
 * A {@link IAtomicLong} that exposes its operations using a {@link ICompletableFuture}
 * so it can be used in the reactive programming model approach.
 *
 * @since 3.2
 */
@Beta
public interface AsyncAtomicLong extends IAtomicLong {

    ICompletableFuture<Long> asyncAddAndGet(long delta);

    ICompletableFuture<Boolean> asyncCompareAndSet(long expect, long update);

    ICompletableFuture<Long> asyncDecrementAndGet();

    ICompletableFuture<Long> asyncGet();

    ICompletableFuture<Long> asyncGetAndAdd(long delta);

    ICompletableFuture<Long> asyncGetAndSet(long newValue);

    ICompletableFuture<Long> asyncIncrementAndGet();

    ICompletableFuture<Long> asyncGetAndIncrement();

    ICompletableFuture<Void> asyncSet(long newValue);

    ICompletableFuture<Void> asyncAlter(IFunction<Long, Long> function);

    ICompletableFuture<Long> asyncAlterAndGet(IFunction<Long, Long> function);

    ICompletableFuture<Long> asyncGetAndAlter(IFunction<Long, Long> function);

    <R> ICompletableFuture<R> asyncApply(IFunction<Long, R> function);
}
