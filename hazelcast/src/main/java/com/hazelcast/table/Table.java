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

package com.hazelcast.table;

import com.hazelcast.bulktransport.BulkTransport;
import com.hazelcast.cluster.Address;


/**
 * This API contains a lot of functionality that normally would be placed
 * over different APIs. But I don't want to jump to a more appropriate solution
 * yet.
 *
 * @param <K>
 * @param <E>
 */
public interface Table<K,E> {

    Pipeline newPipeline();

    void upsert(E item);

    void upsertAll(E[] items);

    void set(byte[] key, byte[] value);

    BulkTransport newBulkTransport(Address address, int parallism);

    byte[] get(byte[] key);

    void bogusQuery();

    void noop();

    void noop(int partitionId);

    void concurrentNoop(int concurrency);

    void concurrentNoop(int concurrency, int partitionId);
}
