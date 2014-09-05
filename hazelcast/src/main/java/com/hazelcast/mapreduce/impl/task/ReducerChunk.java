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

package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.nio.Address;

import java.util.Map;

/**
 * This class represents a chunk of emitted values (maybe already pre-combined) and offered
 * for reducing to the {@link com.hazelcast.mapreduce.impl.task.ReducerTask}.
 *
 * @param <Key>   type of the emitted key
 * @param <Chunk> type of the intermediate chunk data
 */
class ReducerChunk<Key, Chunk> {

    final Map<Key, Chunk> chunk;
    final int partitionId;
    final Address sender;

    ReducerChunk(Map<Key, Chunk> chunk, int partitionId, Address sender) {
        this.chunk = chunk;
        this.sender = sender;
        this.partitionId = partitionId;
    }

}
