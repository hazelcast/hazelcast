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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.kinesis.impl.KinesisUtil;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static java.util.concurrent.TimeUnit.MINUTES;

class ShardTracker {

    /**
     * We consider shards to be expired if they haven't been detected at all
     * (neither as OPEN, nor as CLOSED) since at least this amount of time.
     */
    static final long EXPIRATION_MS = MINUTES.toMillis(10);

    private final Map<String, TrackingInfo> info = new HashMap<>();
    private final HashRange[] rangePartitions;

    ShardTracker(HashRange[] rangePartitions) {
        this.rangePartitions = rangePartitions;
    }

    public void addUndetected(String shardId, BigInteger startingHashKey, long currentTimeMs) {
        assert !info.containsKey(shardId);
        info.put(shardId, new TrackingInfo(findOwner(startingHashKey), currentTimeMs));
    }

    public Map<Shard, Integer> markDetections(Set<Shard> shards, long currentTimeMs) {
        Map<Shard, Integer> newShards = Collections.emptyMap();
        for (Shard shard : shards) {
            String shardId = shard.getShardId();
            TrackingInfo trackingInfo = info.get(shardId);
            if (trackingInfo == null) {
                int owner = findOwner(new BigInteger(shard.getHashKeyRange().getStartingHashKey()));
                trackingInfo = new TrackingInfo(owner, currentTimeMs);
                info.put(shardId, trackingInfo);
            }
            boolean firstDetection = trackingInfo.markDetection(currentTimeMs);
            if (firstDetection) {
                if (newShards.isEmpty()) {
                    newShards = new HashMap<>();
                }
                newShards.put(shard, trackingInfo.getOwner());
            }
        }
        return newShards;
    }

    private int findOwner(BigInteger startingHashKey) {
        for (int i = 0; i < rangePartitions.length; i++) {
            HashRange range = rangePartitions[i];
            if (KinesisUtil.shardBelongsToRange(startingHashKey, range)) {
                return i;
            }
        }
        throw new JetException("Programming error, shard not covered by any hash range");
    }

    public Map<String, Integer> removeExpiredShards(long currentTimeMs) {
        Map<String, Integer> expiredShards = Collections.emptyMap();
        Iterator<Map.Entry<String, TrackingInfo>> it = info.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, TrackingInfo> e =  it.next();
            TrackingInfo trackingInfo = e.getValue();
            if (trackingInfo.isExpired(currentTimeMs)) {
                String shardId = e.getKey();
                int owner = trackingInfo.getOwner();

                if (expiredShards.isEmpty()) {
                    expiredShards = new HashMap<>();
                }
                expiredShards.put(shardId, owner);

                it.remove();
            }
        }
        return expiredShards;
    }

    private static final class TrackingInfo {

        private final int owner;

        private long lastUpdateTimeMs;
        private boolean detected;

        TrackingInfo(int owner, long currentTimeMs) {
            this.owner = owner;

            this.lastUpdateTimeMs = currentTimeMs;
            this.detected = false;
        }

        boolean isExpired(long currentTimeMs) {
            return currentTimeMs - lastUpdateTimeMs > EXPIRATION_MS;
        }

        boolean markDetection(long currentTimeMs) {
            boolean firstDetection = !detected;
            lastUpdateTimeMs = currentTimeMs;
            detected = true;
            return firstDetection;
        }

        int getOwner() {
            return owner;
        }
    }

}
