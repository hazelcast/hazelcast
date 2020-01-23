/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.log;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.log.impl.UsageInfo;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 *
 * https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying
 *
 * @param <E>
 */
public interface Log<E> extends DistributedObject {

    void put(E item);

    CompletableFuture<Void> asyncPut(E item);

    long put(int partition, E item);

    CompletableFuture<Long> asyncPut(int partition, E item);

    CompletableFuture<Void> supplyAsync(int partition, Supplier<E> supplier);

    /**
     * Very useful for filling the data-structure with data for testing purposes.
     *
     * @param supplier
     */
    void supply(Supplier<E> supplier);

    E get(int partition, long sequence);

    CompletableFuture<E> asyncGet(int partition, long sequence);

    Optional<E> reduce(int partition, BinaryOperator<E> accumulator);

    void clear(int partition);

    void clear();

    UsageInfo usage(int partition);

    UsageInfo usage();

    long count();

    CloseableIterator<E> localIterator(int partition);
}
