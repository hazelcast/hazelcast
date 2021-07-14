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

package com.hazelcast.jet.kinesis.impl;

import com.amazonaws.services.kinesis.model.Shard;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.kinesis.impl.source.HashRange;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class KinesisUtil {

    private KinesisUtil() { }

    public static <T> T readResult(Future<T> future) throws Throwable {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JetException("Interrupted while waiting for results");
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    public static boolean shardBelongsToRange(@Nonnull BigInteger startingHashKey, @Nonnull HashRange range) {
        return range.contains(startingHashKey);
    }

    public static boolean shardBelongsToRange(@Nonnull Shard shard, @Nonnull HashRange range) {
        BigInteger startingHashKey = new BigInteger(shard.getHashKeyRange().getStartingHashKey());
        return shardBelongsToRange(startingHashKey, range);
    }
}
