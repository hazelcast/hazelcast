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

/**
 * A Internal Transaction interface that adds the ability to track TransactionRecords.
 *
 * E.g. if 3 puts on a transactional map and 3 adds on a transactional-queue are done, there will
 * be 5 records for that transaction.
 */
public interface InternalTransaction extends Transaction {

    void add(TransactionRecord record);

    void remove(Object key);

    TransactionRecord get(Object key);

    String getOwnerUuid();
}
