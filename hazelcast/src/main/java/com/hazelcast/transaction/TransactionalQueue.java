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

package com.hazelcast.transaction;

import com.hazelcast.collection.BaseQueue;
import com.hazelcast.collection.IQueue;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

/**
 * Transactional implementation of {@link BaseQueue}.
 *
 * @param <E> the type of elements held in this collection
 * @see BaseQueue
 * @see IQueue
 */
public interface TransactionalQueue<E> extends TransactionalObject, BaseQueue<E> {

    /**
     * {@inheritDoc}
     */
    @Override
    boolean offer(@Nonnull E e);

    /**
     * {@inheritDoc}
     */
    @Override
    boolean offer(@Nonnull E e, long timeout, @Nonnull TimeUnit unit) throws InterruptedException;

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    E take() throws InterruptedException;

    /**
     * {@inheritDoc}
     */
    @Override
    E poll();

    /**
     * {@inheritDoc}
     */
    @Override
    E poll(long timeout, @Nonnull TimeUnit unit) throws InterruptedException;

    E peek();

    E peek(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * {@inheritDoc}
     */
    @Override
    int size();
}
