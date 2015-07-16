/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.transaction.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.Future;

/**
 * Every change made in a transaction is recorded on the transaction using a transaction-record.
 *
 * @see InternalTransaction
 */
public interface TransactionRecord extends DataSerializable {

    Future prepare(NodeEngine nodeEngine);

    Future commit(NodeEngine nodeEngine);

    Future rollback(NodeEngine nodeEngine);

    void commitAsync(NodeEngine nodeEngine, ExecutionCallback callback);

    void rollbackAsync(NodeEngine nodeEngine, ExecutionCallback callback);
}
