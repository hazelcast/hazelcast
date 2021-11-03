/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.internal.util.MutableInteger;
import com.hazelcast.spi.impl.operationservice.Operation;

import static java.lang.Integer.getInteger;

public interface ChunkSupplier {

    String PROP_MAX_MIGRATING_DATA = "hazelcast.migrating.data.size";

    int DEFAULT_MAX_MIGRATING_DATA = 2 << 11;

    int MAX_MIGRATING_DATA = getInteger(PROP_MAX_MIGRATING_DATA, DEFAULT_MAX_MIGRATING_DATA);

    void useCounter(MutableInteger byteCounter);

    Operation nextChunk();

    boolean hasMoreChunks();

    boolean hasReachedMaxSize();
}
