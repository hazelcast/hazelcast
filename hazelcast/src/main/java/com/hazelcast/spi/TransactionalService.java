/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.spi;

import com.hazelcast.spi.annotation.ExecutedBy;
import com.hazelcast.spi.annotation.ThreadType;
import com.hazelcast.spi.exception.TransactionException;

public interface TransactionalService {

    @ExecutedBy(ThreadType.PARTITION_THREAD)
    void prepare(String txnId, int partitionId) throws TransactionException;

    @ExecutedBy(ThreadType.PARTITION_THREAD)
    void commit(String txnId, int partitionId) throws TransactionException;

    @ExecutedBy(ThreadType.PARTITION_THREAD)
    void rollback(String txnId, int partitionId) throws TransactionException;
}
