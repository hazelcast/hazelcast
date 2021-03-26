/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kinesis.impl.source;

import com.amazonaws.services.kinesis.model.Shard;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

class ShardQueue {

    private final Queue<Object> queue = new LinkedBlockingQueue<>();

    public void addAdded(@Nonnull Shard shard) {
        queue.offer(shard);
    }

    public void addExpired(@Nonnull String shardId) {
        queue.offer(shardId);
    }

    /**
     * Polls the queue and return an added shard, if the next item is an added
     * shard. Returns null if the next item is an expired shard or if there's
     * no next item.
     */
    @Nullable
    public Shard pollAdded() {
        return get(Shard.class);
    }

    /**
     * Polls the queue and return an expired shard, if the next item is an
     * expired shard. Returns null if the next item is an added shard or if
     * there's no next item.
     */
    @Nullable
    public String pollExpired() {
        return get(String.class);
    }

    @Nullable
    private <T> T get(Class<T> clazz) {
        // the queue is read only by a single thread
        return clazz.isInstance(queue.peek()) ? clazz.cast(queue.poll()) : null;
    }
}
