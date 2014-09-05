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

import com.hazelcast.transaction.TransactionalObject;

import java.util.concurrent.TimeUnit;

/**
 * Transactional implementation of {@link BaseQueue}.
 *
 * @see BaseQueue
 * @see IQueue
 * @param <E>
 */
public interface TransactionalQueue<E> extends TransactionalObject, BaseQueue<E> {

    /**
     * {@inheritDoc}
     */
    boolean offer(E e);

    /**
     * {@inheritDoc}
     */
    boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * {@inheritDoc}
     */
    E take() throws InterruptedException;

    /**
     * {@inheritDoc}
     */
    E poll();

    /**
     * {@inheritDoc}
     */
    E poll(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * {@inheritDoc}
     */
    E peek();

    /**
     * {@inheritDoc}
     */
    E peek(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * {@inheritDoc}
     */
    int size();

}
