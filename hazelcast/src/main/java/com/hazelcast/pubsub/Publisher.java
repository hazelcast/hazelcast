/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.pubsub;

import com.hazelcast.core.TpcProxy;

/**
 * A publisher to a specific topic.
 * <p>
 * todo: async versions.
 */
public interface Publisher extends TpcProxy {

    byte SYNC_NONE = 0;
    byte SYNC_FSYNC = 1;
    byte SYNC_FDATASYNC = 2;

    /**
     * Writes a message to a random partition.
     *
     * @param message
     * @param syncOption
     */
    void publish(byte[] message, byte syncOption);

    // todo: this API is very inefficient because we impose a byte-array.
    // Perhaps expose a version with a byte buffer.java

    /**
     * Writes a message to the partition with the given id.
     *
     * @param partitionId
     * @param message
     * @param syncOption
     */
    void publish(int partitionId, byte[] message, byte syncOption);
}
