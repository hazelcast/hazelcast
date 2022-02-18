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

/**
 * Contains the logic that is going to be executed within a transaction. In practice, the
 * implementation will be an anonymous inner task.
 * <p>
 * Unlike the {@link Runnable} and {@link java.util.concurrent.Callable}, the {@link TransactionalTask}
 * will run on the caller thread.
 *
 * @author mdogan 3/6/13
 * @see com.hazelcast.core.HazelcastInstance#executeTransaction(TransactionalTask)
 * @see com.hazelcast.core.HazelcastInstance#executeTransaction(TransactionOptions, TransactionalTask)
 */
@FunctionalInterface
public interface TransactionalTask<T> {

    /**
     * Executes the transactional logic.
     *
     * @param context the TransactionalTaskContext that provides access to the transaction and to the
     *                transactional resource like the {@link TransactionalMap}.
     * @return the result of the task
     * @throws TransactionException if transaction error happens while executing this task.
     */
    T execute(TransactionalTaskContext context) throws TransactionException;

}
